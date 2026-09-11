// Package gateway turns a raw TUN file descriptor (as handed out by
// Android's VpnService, or any other source of whole IP packets) into TCP
// connections dialed through a Dialer - the same role socks5.SOCKS5Server
// plays for desktop clients, but for callers that hand over raw device
// traffic instead of SOCKS5 CONNECT requests.
package gateway

import (
	"fmt"
	"io"
	"net"
	"sync"
	"sync/atomic"
	"time"

	"gvisor.dev/gvisor/pkg/tcpip"
	"gvisor.dev/gvisor/pkg/tcpip/adapters/gonet"
	"gvisor.dev/gvisor/pkg/tcpip/network/ipv4"
	"gvisor.dev/gvisor/pkg/tcpip/stack"
	"gvisor.dev/gvisor/pkg/tcpip/transport/tcp"
	"gvisor.dev/gvisor/pkg/tcpip/transport/udp"
	"gvisor.dev/gvisor/pkg/waiter"

	"universal-bypass-tool/tunnel"
	"universal-bypass-tool/utils"
)

// Dialer is satisfied by *tunnel.TCPTunnel: it dials an arbitrary
// destination out through whatever covert transport that tunnel wraps.
type Dialer interface {
	DialTCP(address string) (net.Conn, error)
	DialUDP(address string) (net.Conn, error)
}

// udpIdleTimeout closes a relayed UDP flow after this long without a
// datagram in either direction - UDP has no FIN/close signal of its own, so
// without this a flow whose local app simply stops sending (rather than
// tearing down its socket) would relay forever.
const udpIdleTimeout = 60 * time.Second

const gatewayNIC = tcpip.NICID(1)

// Server runs a gvisor stack in transparent-proxy mode: its one NIC accepts
// packets addressed to any destination (promiscuous + spoofing, the
// standard gvisor-as-tun2socks pattern), intercepts new TCP connections and
// UDP flows via forwarders, and relays each one through Dialer - DNS
// (UDP/53) gets its own request/response framing (see relayDNS), everything
// else gets a generic bidirectional datagram relay (see relayUDP). Any IPv6
// (this stack only registers ipv4) is left unhandled, which fails closed
// rather than leaking outside the tunnel.
type Server struct {
	dialer      Dialer
	dnsUpstream string

	gvisorStack *stack.Stack
	linkEP      *tunnel.TunnelLinkEndpoint
	closed      atomic.Bool
}

// NewServer builds a gateway that relays through dialer. dnsUpstream is the
// host (optionally host:port, defaulting to :53) of the DNS-over-TCP
// resolver used for intercepted DNS queries; it is dialed through the same
// Dialer, so resolution goes through the tunnel like everything else.
func NewServer(dialer Dialer, dnsUpstream string) *Server {
	if dnsUpstream == "" {
		dnsUpstream = "77.88.8.8:53"
	} else if _, _, err := net.SplitHostPort(dnsUpstream); err != nil {
		dnsUpstream = net.JoinHostPort(dnsUpstream, "53")
	}
	return &Server{dialer: dialer, dnsUpstream: dnsUpstream}
}

// Start wires up the gvisor stack and begins pumping packets: tunReader is
// read in a background goroutine (one raw IP packet per Read, matching a
// TUN device's framing) and injected into the stack; packets the stack
// wants to emit are written to tunWriter. Start returns once the stack is
// ready; the pump and per-connection relays keep running until Close.
func (s *Server) Start(tunReader io.Reader, tunWriter io.Writer) error {
	s.gvisorStack = stack.New(stack.Options{
		NetworkProtocols:   []stack.NetworkProtocolFactory{ipv4.NewProtocol},
		TransportProtocols: []stack.TransportProtocolFactory{tcp.NewProtocol, udp.NewProtocol},
	})

	linkEP := tunnel.NewTunnelLinkEndpoint()
	linkEP.SetOutgoingPacketHandler(func(data []byte) {
		if _, err := tunWriter.Write(data); err != nil {
			utils.Debugf("[GATEWAY] tun write error: %v", err)
		}
	})
	s.linkEP = linkEP

	if err := s.gvisorStack.CreateNIC(gatewayNIC, linkEP); err != nil {
		return fmt.Errorf("create gateway NIC: %v", err)
	}
	if err := s.gvisorStack.SetPromiscuousMode(gatewayNIC, true); err != nil {
		return fmt.Errorf("enable promiscuous mode: %v", err)
	}
	if err := s.gvisorStack.SetSpoofing(gatewayNIC, true); err != nil {
		return fmt.Errorf("enable spoofing: %v", err)
	}

	tcpForwarder := tcp.NewForwarder(s.gvisorStack, 0, 2048, s.handleTCP)
	s.gvisorStack.SetTransportProtocolHandler(tcp.ProtocolNumber, tcpForwarder.HandlePacket)

	udpForwarder := udp.NewForwarder(s.gvisorStack, s.handleUDP)
	s.gvisorStack.SetTransportProtocolHandler(udp.ProtocolNumber, udpForwarder.HandlePacket)

	go s.readTunLoop(tunReader)

	return nil
}

// Close tears the gateway's gvisor stack down. It does not close
// tunReader/tunWriter - the caller owns that file's lifecycle (in the
// mobile package, the TUN fd handed in from Android).
func (s *Server) Close() {
	s.closed.Store(true)
	if s.gvisorStack != nil {
		s.gvisorStack.Destroy()
	}
}

func (s *Server) readTunLoop(r io.Reader) {
	buf := make([]byte, 1500+64)
	for {
		n, err := r.Read(buf)
		if err != nil {
			if !s.closed.Load() {
				utils.Debugf("[GATEWAY] tun read error: %v", err)
			}
			return
		}
		if n == 0 {
			continue
		}
		pkt := make([]byte, n)
		copy(pkt, buf[:n])
		s.linkEP.InjectInbound(pkt)
	}
}

func (s *Server) handleTCP(r *tcp.ForwarderRequest) {
	id := r.ID()

	var wq waiter.Queue
	ep, err := r.CreateEndpoint(&wq)
	if err != nil {
		utils.Debugf("[GATEWAY] tcp handshake failed for %s:%d: %v", id.LocalAddress, id.LocalPort, err)
		r.Complete(true)
		return
	}
	r.Complete(false)

	localConn := gonet.NewTCPConn(&wq, ep)
	dest := net.JoinHostPort(id.LocalAddress.String(), fmt.Sprint(id.LocalPort))

	go s.relayTCP(localConn, dest)
}

func (s *Server) relayTCP(localConn net.Conn, dest string) {
	defer localConn.Close()

	remoteConn, err := s.dialer.DialTCP(dest)
	if err != nil {
		utils.Debugf("[GATEWAY] dial %s failed: %v", dest, err)
		return
	}
	defer remoteConn.Close()

	var wg sync.WaitGroup
	wg.Add(2)

	go func() {
		defer wg.Done()
		defer remoteConn.Close()
		io.Copy(remoteConn, localConn)
	}()
	go func() {
		defer wg.Done()
		defer localConn.Close()
		io.Copy(localConn, remoteConn)
	}()

	wg.Wait()
}

// handleUDP accepts every UDP flow: port 53 (DNS) gets the request/response
// framing in relayDNS, everything else gets a generic bidirectional relay
// in relayUDP. gvisor's forwarder creates one "connected" endpoint per
// distinct 5-tuple (its peer fixed to whoever sent the first datagram) and
// keeps delivering that flow's later datagrams to the same endpoint, so
// relayUDP can treat it like any other long-lived connection.
func (s *Server) handleUDP(r *udp.ForwarderRequest) bool {
	id := r.ID()

	var wq waiter.Queue
	ep, err := r.CreateEndpoint(&wq)
	if err != nil {
		utils.Debugf("[GATEWAY] udp endpoint failed for %s:%d: %v", id.LocalAddress, id.LocalPort, err)
		return false
	}

	localConn := gonet.NewUDPConn(&wq, ep)
	if id.LocalPort == 53 {
		go s.relayDNS(localConn)
		return true
	}

	dest := net.JoinHostPort(id.LocalAddress.String(), fmt.Sprint(id.LocalPort))
	go s.relayUDP(localConn, dest)
	return true
}

// relayUDP bridges one local UDP flow to dest through the tunnel, copying
// datagrams in both directions until either side errors, closes, or goes
// silent for longer than udpIdleTimeout - see that constant's doc comment
// for why a timeout is needed at all for a protocol with no close signal.
func (s *Server) relayUDP(localConn net.Conn, dest string) {
	defer localConn.Close()

	remoteConn, err := s.dialer.DialUDP(dest)
	if err != nil {
		utils.Debugf("[GATEWAY] udp dial %s failed: %v", dest, err)
		return
	}
	defer remoteConn.Close()

	var wg sync.WaitGroup
	wg.Add(2)

	go func() {
		defer wg.Done()
		defer remoteConn.Close()
		copyDatagrams(remoteConn, localConn)
	}()
	go func() {
		defer wg.Done()
		defer localConn.Close()
		copyDatagrams(localConn, remoteConn)
	}()

	wg.Wait()
}

// copyDatagrams relays src -> dst one datagram per Read/Write, resetting
// src's read deadline after every datagram - unlike io.Copy, this is what
// lets an idle (not closed) flow time out instead of relaying forever.
func copyDatagrams(dst, src net.Conn) {
	buf := make([]byte, 65535)
	for {
		src.SetReadDeadline(time.Now().Add(udpIdleTimeout))
		n, err := src.Read(buf)
		if err != nil {
			return
		}
		if _, err := dst.Write(buf[:n]); err != nil {
			return
		}
	}
}
