package tunnel

import (
	"fmt"
	"io"
	"net"
	"sync"
	"sync/atomic"
	"time"

	"gvisor.dev/gvisor/pkg/tcpip"
	"gvisor.dev/gvisor/pkg/tcpip/adapters/gonet"
	"gvisor.dev/gvisor/pkg/tcpip/header"
	"gvisor.dev/gvisor/pkg/tcpip/network/ipv4"
	"gvisor.dev/gvisor/pkg/tcpip/stack"
	"gvisor.dev/gvisor/pkg/tcpip/transport/tcp"
	"gvisor.dev/gvisor/pkg/tcpip/transport/udp"
	"gvisor.dev/gvisor/pkg/waiter"

	"universal-bypass-tool/transport"
	"universal-bypass-tool/utils"
)

// ExitMode picks how an exit-node TCPTunnel reaches the real internet - see
// NewTCPTunnelMode.
type ExitMode int

const (
	// ExitModeRaw forwards raw IP packets through a real raw socket, backed
	// by gvisor's own NAT/forwarding (SetForwardingDefaultAndAllNICs) - the
	// long-standing default. Needs root and a raw socket, and carries any
	// IP protocol the client sends (this is how general UDP relay, not just
	// TCP, currently works) - but that same protocol-agnostic forwarding is
	// also what let an unexpected ICMP packet reach gvisor's NAT code and
	// crash it (nil pointer deref) before InjectInbound started filtering
	// non-TCP packets out.
	ExitModeRaw ExitMode = iota
	// ExitModeProxy terminates each TCP flow locally in gvisor (via a
	// tcp.Forwarder) and re-originates it with an ordinary net.Dial to the
	// real destination - no root, no raw socket, no iptables RST-drop rule,
	// and no path through gvisor's NAT code at all (so the ICMP crash class
	// of bug can't recur here even in principle). Ported from upstream
	// (p1neappleXpress/OpenFlux), which defaults to it. Not the default
	// here yet: it only forwards TCP - a UDP packet arriving under this mode
	// gets gvisor's own default response for an unclaimed port (an ICMP
	// port-unreachable, which incidentally makes QUIC-preferring apps fall
	// back to TCP fast instead of stalling - upstream hand-crafts this same
	// ICMP reply on its gvisor-free iOS path; here it's just what the stack
	// already does when nothing registers a UDP handler) rather than being
	// relayed - switching the default would silently drop that for anyone
	// depending on it today.
	ExitModeProxy
)

func (m ExitMode) String() string {
	if m == ExitModeProxy {
		return "proxy"
	}
	return "raw"
}

// ParseExitMode parses the --mode flag's value.
func ParseExitMode(s string) (ExitMode, error) {
	switch s {
	case "", "raw":
		return ExitModeRaw, nil
	case "proxy":
		return ExitModeProxy, nil
	default:
		return ExitModeRaw, fmt.Errorf("unknown mode %q (want raw|proxy)", s)
	}
}

type TCPTunnel struct {
	gvisorStack *stack.Stack
	tunnelEP    *TunnelLinkEndpoint
	transport   transport.Transport
	isExitNode  bool
	exitMode    ExitMode
	rawEP       *RawSocketEndpoint
	startTime   time.Time
	packetCount atomic.Uint64
	stopStats   chan struct{}
}

// NewTCPTunnel is NewTCPTunnelMode with ExitModeRaw - the long-standing
// default, unchanged for every existing caller.
func NewTCPTunnel(trans transport.Transport, isExitNode bool) *TCPTunnel {
	return NewTCPTunnelMode(trans, isExitNode, ExitModeRaw)
}

// NewTCPTunnelMode is NewTCPTunnel with an explicit exit mode (see ExitMode);
// mode is ignored when isExitNode is false.
func NewTCPTunnelMode(trans transport.Transport, isExitNode bool, mode ExitMode) *TCPTunnel {
	t := &TCPTunnel{
		transport:  trans,
		isExitNode: isExitNode,
		exitMode:   mode,
		startTime:  time.Now(),
		stopStats:  make(chan struct{}),
	}

	utils.Debugf("[TUNNEL] Net stack init...")
	t.gvisorStack = stack.New(stack.Options{
		NetworkProtocols:   []stack.NetworkProtocolFactory{ipv4.NewProtocol},
		TransportProtocols: []stack.TransportProtocolFactory{tcp.NewProtocol, udp.NewProtocol},
	})

	// Max bounds throughput at Max*8/RTT - at this tunnel's ~200-400ms RTT,
	// the old 1MB capped a connection to ~25-30 Mbit/s from window
	// exhaustion alone, well under CPU/RAM limits.
	if err := t.gvisorStack.SetTransportProtocolOption(tcp.ProtocolNumber,
		&tcpip.TCPReceiveBufferSizeRangeOption{Min: 65536, Default: 262144, Max: 8 * 1024 * 1024}); err != nil {
		utils.Debugf("[TUNNEL] Failed to set recv buffer: %v", err)
	}
	if err := t.gvisorStack.SetTransportProtocolOption(tcp.ProtocolNumber,
		&tcpip.TCPSendBufferSizeRangeOption{Min: 65536, Default: 262144, Max: 8 * 1024 * 1024}); err != nil {
		utils.Debugf("[TUNNEL] Failed to set send buffer: %v", err)
	}
	// gvisor's default MinRTO (200ms, tuned for a real NIC) is far shorter
	// than a real round trip through this NIC's actual backing channel: a
	// segment written here goes out over trans.Send, across the covert
	// channel's own HTTP/WebSocket relay (base64/JSON encoding, a real
	// network hop each way, and on Volga a batching window on top), and the
	// ack for it comes back the same way - regularly well over 200ms even
	// when nothing is actually lost. Below this floor, gvisor's TCP treats
	// ordinary channel latency as packet loss and retransmits data that's
	// still legitimately in flight - genuinely re-sent over the real
	// channel, so it counts as real traffic, just entirely wasted. True
	// loss at this layer is rare (the channel underneath is TCP-backed
	// itself), so trading a slower reaction to real loss for eliminating
	// false-positive retransmits is a clear net win here.
	minRTO := tcpip.TCPMinRTOOption(1500 * time.Millisecond)
	if err := t.gvisorStack.SetTransportProtocolOption(tcp.ProtocolNumber, &minRTO); err != nil {
		utils.Debugf("[TUNNEL] Failed to set min RTO: %v", err)
	}

	tunnelEP := NewTunnelLinkEndpoint()
	tunnelEP.SetOutgoingPacketHandler(func(data []byte) {
		trans.Send(data)
	})
	t.tunnelEP = tunnelEP

	tunnelNIC := tcpip.NICID(1)
	if err := t.gvisorStack.CreateNIC(tunnelNIC, tunnelEP); err != nil {
		utils.Debugf("[TUNNEL] CreateNIC tunnel error: %v", err)
	}

	if isExitNode {
		if mode == ExitModeProxy {
			t.setupExitNodeProxy(tunnelNIC)
		} else {
			t.setupExitNodeRaw(tunnelNIC)
		}
	} else {
		t.setupClient(tunnelNIC)
	}

	trans.Receive(func(data []byte) {
		tunnelEP.InjectInbound(data)
	})

	go t.printStats()
	return t
}

// setupExitNodeProxy terminates each client TCP flow locally in gvisor and
// re-originates it with a plain net.Dial to the real destination - see
// ExitModeProxy's doc comment. SetPromiscuousMode+SetSpoofing let this NIC
// accept and reply to a SYN for ANY destination IP (every real address the
// client dials, none of which this NIC actually owns) without gvisor
// rejecting it as foreign traffic first.
func (t *TCPTunnel) setupExitNodeProxy(tunnelNIC tcpip.NICID) {
	utils.Debugf("[TUNNEL] EXIT NODE - proxy mode (no raw socket, no root)")

	if err := t.gvisorStack.SetPromiscuousMode(tunnelNIC, true); err != nil {
		utils.Debugf("[TUNNEL] SetPromiscuousMode error: %v", err)
	}
	if err := t.gvisorStack.SetSpoofing(tunnelNIC, true); err != nil {
		utils.Debugf("[TUNNEL] SetSpoofing error: %v", err)
	}
	t.gvisorStack.AddRoute(tcpip.Route{
		Destination: header.IPv4EmptySubnet,
		NIC:         tunnelNIC,
	})

	fwd := tcp.NewForwarder(t.gvisorStack, 0, 8192, t.handleExitTCP)
	t.gvisorStack.SetTransportProtocolHandler(tcp.ProtocolNumber, fwd.HandlePacket)
	// No udp.NewForwarder here - see ExitModeProxy's doc comment: this
	// leaves UDP unclaimed, and gvisor answers it with its own default
	// port-unreachable response rather than this mode relaying it.
}

// exitTCPMaxFlows bounds how many proxy-mode flows this process relays at
// once - handleExitTCP has no other backpressure once a flow is dialed (the
// forwarder's 8192 backlog only limits pending SYNs, not established
// flows), so without a cap a burst of destinations that accept a connection
// and then go silent (a common, non-malicious internet condition - not just
// an attack) would otherwise accumulate goroutines/sockets/gvisor endpoints
// without bound.
const exitTCPMaxFlows = 4096

// exitTCPIdleTimeout closes a proxy-mode flow that's been silent (no bytes
// either direction) this long - net.DialTimeout only bounds the initial
// connect, so without this a remote peer that accepts the connection and
// then never sends or reads again (a blackholed NAT/firewall, a hung
// server) would otherwise leak its goroutines/socket/gvisor endpoint for
// the tunnel's entire remaining lifetime.
const exitTCPIdleTimeout = 5 * time.Minute

var exitTCPSemaphore = make(chan struct{}, exitTCPMaxFlows)

// handleExitTCP accepts one client flow's TCP handshake locally (via
// r.CreateEndpoint, gvisor's side of the connection) and relays it to the
// real destination with an ordinary net.Dial - the entire point of proxy
// mode: nothing here touches a raw socket or gvisor's NAT/forwarding code.
func (t *TCPTunnel) handleExitTCP(r *tcp.ForwarderRequest) {
	id := r.ID()
	dest := net.JoinHostPort(id.LocalAddress.String(), fmt.Sprintf("%d", id.LocalPort))

	var wq waiter.Queue
	ep, tErr := r.CreateEndpoint(&wq)
	if tErr != nil {
		utils.Debugf("[EXIT] proxy CreateEndpoint %s: %v", dest, tErr)
		r.Complete(true)
		return
	}
	r.Complete(false)
	local := gonet.NewTCPConn(&wq, ep)

	select {
	case exitTCPSemaphore <- struct{}{}:
	default:
		utils.Debugf("[EXIT] proxy %s rejected: too many concurrent flows (%d)", dest, exitTCPMaxFlows)
		local.Close()
		return
	}

	go func() {
		defer func() { <-exitTCPSemaphore }()

		remote, err := net.DialTimeout("tcp", dest, 10*time.Second)
		if err != nil {
			utils.Debugf("[EXIT] proxy dial %s failed: %v", dest, err)
			local.Close()
			return
		}
		if tc, ok := remote.(*net.TCPConn); ok {
			_ = tc.SetNoDelay(true)
			_ = tc.SetKeepAlive(true)
			_ = tc.SetKeepAlivePeriod(30 * time.Second)
		}
		utils.Debugf("[EXIT] proxy %s connected", dest)

		var wg sync.WaitGroup
		wg.Add(2)
		go func() {
			defer wg.Done()
			idleCopy(remote, local, exitTCPIdleTimeout)
		}()
		go func() {
			defer wg.Done()
			idleCopy(local, remote, exitTCPIdleTimeout)
		}()
		wg.Wait()
		local.Close()
		remote.Close()
	}()
}

// idleCopy is io.Copy with a per-read/write idle deadline: src.Read (and the
// dst.Write that follows a successful read) must make progress within
// idleTimeout or the copy gives up, instead of blocking forever on a peer
// that's gone silent without closing the connection.
func idleCopy(dst, src net.Conn, idleTimeout time.Duration) {
	buf := make([]byte, 32*1024)
	for {
		_ = src.SetReadDeadline(time.Now().Add(idleTimeout))
		n, rerr := src.Read(buf)
		if n > 0 {
			_ = dst.SetWriteDeadline(time.Now().Add(idleTimeout))
			if _, werr := dst.Write(buf[:n]); werr != nil {
				return
			}
		}
		if rerr != nil {
			if rerr != io.EOF {
				utils.Debugf("[EXIT] proxy idle copy read error: %v", rerr)
			}
			return
		}
	}
}

// setupExitNodeRaw forwards raw IP packets through a real raw socket via
// gvisor's own NAT (SetForwardingDefaultAndAllNICs) - needs root, and the
// kernel's own TCP stack (which owns no socket for these gvisor-terminated
// connections) sends a real RST on every reply unless the deploy also runs
// the RST-drop iptables rule mentioned on localIPOverride/SetLocalIP above.
func (t *TCPTunnel) setupExitNodeRaw(tunnelNIC tcpip.NICID) {
	localIP := getLocalIP()
	utils.Debugf("[TUNNEL] EXIT NODE - raw mode, local IP: %s", localIP)

	rawEP, err := NewRawSocketEndpoint(tcpip.NICID(2))
	if err != nil {
		// Most commonly: not running as root. Falling back instead of
		// leaving a half-configured tunnel (a tunnel NIC with no route to
		// anywhere) means forgetting sudo degrades to a working but
		// UDP-relay-less exit node instead of a silently broken one.
		utils.Debugf("[TUNNEL] raw socket error (mode raw needs root): %v", err)
		utils.Debugf("[TUNNEL] falling back to proxy mode")
		t.exitMode = ExitModeProxy
		t.setupExitNodeProxy(tunnelNIC)
		return
	}

	t.rawEP = rawEP
	rawEP.SetTransportSender(func(data []byte) {
		t.transport.Send(data)
	})

	internetNIC := tcpip.NICID(2)
	if err := t.gvisorStack.CreateNIC(internetNIC, rawEP); err != nil {
		utils.Debugf("[TUNNEL] CreateNIC internet error: %v", err)
		return
	}

	var ipBytes [4]byte
	fmt.Sscanf(localIP, "%d.%d.%d.%d", &ipBytes[0], &ipBytes[1], &ipBytes[2], &ipBytes[3])
	internetAddr := tcpip.AddrFrom4(ipBytes)
	t.gvisorStack.AddProtocolAddress(internetNIC, tcpip.ProtocolAddress{
		Protocol: ipv4.ProtocolNumber,
		AddressWithPrefix: tcpip.AddressWithPrefix{
			Address:   internetAddr,
			PrefixLen: 24,
		},
	}, stack.AddressProperties{})

	t.gvisorStack.SetForwardingDefaultAndAllNICs(ipv4.ProtocolNumber, true)
	t.gvisorStack.AddRoute(tcpip.Route{
		Destination: header.IPv4EmptySubnet,
		NIC:         internetNIC,
	})

	tunnelSubnet := tcpip.AddressWithPrefix{
		Address:   tcpip.AddrFrom4([4]byte{10, 10, 10, 0}),
		PrefixLen: 24,
	}.Subnet()
	t.gvisorStack.AddRoute(tcpip.Route{
		Destination: tunnelSubnet,
		NIC:         tunnelNIC,
	})
}

func (t *TCPTunnel) setupClient(tunnelNIC tcpip.NICID) {
	clientAddr := tcpip.AddrFrom4([4]byte{10, 10, 10, 2})
	t.gvisorStack.AddProtocolAddress(tunnelNIC, tcpip.ProtocolAddress{
		Protocol: ipv4.ProtocolNumber,
		AddressWithPrefix: tcpip.AddressWithPrefix{
			Address:   clientAddr,
			PrefixLen: 24,
		},
	}, stack.AddressProperties{})

	t.gvisorStack.AddRoute(tcpip.Route{
		Destination: header.IPv4EmptySubnet,
		NIC:         tunnelNIC,
	})
}

// ExitMode reports the exit mode this tunnel actually ended up running in -
// which can differ from what NewTCPTunnelMode was asked for if raw-socket
// setup failed and silently fell back to proxy mode (see
// setupExitNodeRaw). Callers that print or act on the requested mode (e.g.
// main.go's startup banner) should read this instead, so a fallback isn't
// reported as if raw mode were actually running.
func (t *TCPTunnel) ExitMode() ExitMode {
	return t.exitMode
}

// SetPortRange restricts the ephemeral ports this tunnel's gvisor stack
// picks for outbound connections to [start, end]. An exit node running one
// TCPTunnel per key all share the same real IP and a raw socket that
// receives every TCP packet addressed to the host - each stack's ephemeral
// port allocator has no idea any of the others exist, so with the default
// (whole) range, two keys' stacks can independently pick the same source
// port for two different real destinations at the same time. Both raw
// sockets see every reply on that port either way, and both keys'
// activePorts tables would independently claim it, delivering one key's
// real traffic into the other's tunnel. Giving every worker a disjoint
// range (see nodeagent's portAllocator) makes that impossible by
// construction instead of merely unlikely. Call before any real traffic
// flows - changing it mid-flight would strand in-progress connections whose
// ports fall outside the new range.
func (t *TCPTunnel) SetPortRange(start, end uint16) {
	if err := t.gvisorStack.SetPortRange(start, end); err != nil {
		utils.Debugf("[TUNNEL] SetPortRange(%d-%d) failed: %v", start, end, err)
	}
}

func (t *TCPTunnel) DialTCP(address string) (net.Conn, error) {
	tcpAddr, err := net.ResolveTCPAddr("tcp", address)
	if err != nil {
		return nil, fmt.Errorf("resolve: %w", err)
	}

	ip := tcpAddr.IP.To4()
	if ip == nil {
		return nil, fmt.Errorf("IPv6 not supported")
	}

	nic := tcpip.NICID(1)
	if t.isExitNode && t.exitMode == ExitModeRaw {
		nic = tcpip.NICID(2)
	}

	conn, err := gonet.DialTCP(t.gvisorStack, tcpip.FullAddress{
		NIC:  nic,
		Addr: tcpip.AddrFrom4([4]byte{ip[0], ip[1], ip[2], ip[3]}),
		Port: uint16(tcpAddr.Port),
	}, ipv4.ProtocolNumber)

	return conn, err
}

// DialUDP mirrors DialTCP but for UDP: it creates a "connected" gvisor UDP
// endpoint bound to the tunnel (or, on an exit node, internet-facing) NIC,
// whose datagrams travel the exact same path as TCP segments do - real
// UDP/IP packets emitted by gvisor, shipped over the covert channel, and
// (on the exit node) IP-forwarded out to the real destination exactly like
// any other forwarded packet, since setupExitNode's forwarding is plain L3
// and never inspects the transport protocol.
func (t *TCPTunnel) DialUDP(address string) (net.Conn, error) {
	udpAddr, err := net.ResolveUDPAddr("udp", address)
	if err != nil {
		return nil, fmt.Errorf("resolve: %w", err)
	}

	ip := udpAddr.IP.To4()
	if ip == nil {
		return nil, fmt.Errorf("IPv6 not supported")
	}

	nic := tcpip.NICID(1)
	if t.isExitNode && t.exitMode == ExitModeRaw {
		nic = tcpip.NICID(2)
	}

	remote := tcpip.FullAddress{
		NIC:  nic,
		Addr: tcpip.AddrFrom4([4]byte{ip[0], ip[1], ip[2], ip[3]}),
		Port: uint16(udpAddr.Port),
	}
	return gonet.DialUDP(t.gvisorStack, nil, &remote, ipv4.ProtocolNumber)
}

func (t *TCPTunnel) ListenTCP(port uint16) (net.Listener, error) {
	return gonet.ListenTCP(t.gvisorStack, tcpip.FullAddress{
		NIC:  1,
		Port: port,
	}, ipv4.ProtocolNumber)
}

func (t *TCPTunnel) printStats() {
	ticker := time.NewTicker(30 * time.Second)
	defer ticker.Stop()

	for {
		select {
		case <-t.stopStats:
			return
		case <-ticker.C:
			stats := t.gvisorStack.Stats()
			utils.Debugf("[STATS] uptime=%v mode=%s packets=%d connected=%d established=%d retrans=%d",
				time.Since(t.startTime).Round(time.Second),
				t.exitMode.String(),
				t.packetCount.Load(),
				stats.TCP.CurrentConnected.Value(),
				stats.TCP.CurrentEstablished.Value(),
				stats.TCP.Retransmits.Value(),
			)
		}
	}
}

// Close tears the tunnel down: stops the background stats loop and destroys
// the gvisor network stack, releasing its endpoints and worker goroutines.
// It does not touch the underlying Transport - callers own that and should
// Stop() it themselves (nodeagent does this when retiring a per-key worker).
func (t *TCPTunnel) Close() {
	close(t.stopStats)
	t.gvisorStack.Destroy()
}

// localIPOverride, when set, is the address raw mode uses as its egress IP
// instead of auto-detecting one. Point it at a dedicated alias IP so the
// RST-drop iptables rule raw mode needs (see setupExitNodeRaw's doc comment
// on why) can be scoped as `-s <ip>` instead of dropping every outbound RST
// on the host - which makes every closed port on the box look "filtered"
// to a port scan instead of "closed", and stops the host resetting any
// OTHER connection of its own. `-m owner --uid-owner` can't fix this either:
// the RSTs are kernel-generated with no owning socket to match.
var localIPOverride string

// SetLocalIP overrides the auto-detected egress IP raw mode uses - see
// localIPOverride. Ignored in proxy mode, which has no raw socket and so
// nothing that needs an RST-drop rule scoped to begin with.
func SetLocalIP(ip string) { localIPOverride = ip }

func getLocalIP() string {
	if localIPOverride != "" {
		return localIPOverride
	}
	conn, err := net.Dial("udp", "8.8.8.8:80")
	if err != nil {
		return "192.168.1.100"
	}
	defer conn.Close()
	localAddr := conn.LocalAddr().(*net.UDPAddr)
	return localAddr.IP.String()
}
