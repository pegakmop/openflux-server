package tunnel

import (
	"bytes"
	"fmt"
	"sync"
	"sync/atomic"
	"syscall"
	"time"

	"gvisor.dev/gvisor/pkg/tcpip"
	"gvisor.dev/gvisor/pkg/tcpip/header"
	"gvisor.dev/gvisor/pkg/tcpip/stack"

	"universal-bypass-tool/network"
	"universal-bypass-tool/utils"
)

type RawSocketEndpoint struct {
	dispatcher stack.NetworkDispatcher
	sendFd     int
	recvFd     int
	// recvUDPFd is a SECOND receive socket, for UDP. One socket cannot cover both: on Linux
	// SOCK_RAW delivers exactly the protocol it was opened with, and IPPROTO_RAW is send-only
	// (it receives nothing). While the only receive socket was IPPROTO_TCP, reply datagrams never
	// reached the tunnel at all — from the client it looked like "UDP does not work", even though
	// the requests did go out: sending uses IPPROTO_RAW, which is not limited to one protocol.
	recvUDPFd       int
	nicID           tcpip.NICID
	localIP         [4]byte
	packetIn        atomic.Uint64
	packetOut       atomic.Uint64
	outgoingSYNs    sync.Map
	activePorts     sync.Map
	sendToTransport func([]byte)
}

func NewRawSocketEndpoint(nicID tcpip.NICID) (*RawSocketEndpoint, error) {
	sendFd, err := syscall.Socket(syscall.AF_INET, syscall.SOCK_RAW, syscall.IPPROTO_RAW)
	if err != nil {
		return nil, fmt.Errorf("send socket failed: %v (need root)", err)
	}

	if err := syscall.SetsockoptInt(sendFd, syscall.IPPROTO_IP, syscall.IP_HDRINCL, 1); err != nil {
		syscall.Close(sendFd)
		return nil, fmt.Errorf("IP_HDRINCL: %v", err)
	}

	recvFd, err := syscall.Socket(syscall.AF_INET, syscall.SOCK_RAW, syscall.IPPROTO_TCP)
	if err != nil {
		syscall.Close(sendFd)
		return nil, fmt.Errorf("recv socket failed: %v (need root)", err)
	}

	addr := &syscall.SockaddrInet4{
		Addr: [4]byte{0, 0, 0, 0},
		Port: 0,
	}
	if err := syscall.Bind(recvFd, addr); err != nil {
		syscall.Close(sendFd)
		syscall.Close(recvFd)
		return nil, fmt.Errorf("bind failed: %v", err)
	}

	recvUDPFd, err := syscall.Socket(syscall.AF_INET, syscall.SOCK_RAW, syscall.IPPROTO_UDP)
	if err != nil {
		syscall.Close(sendFd)
		syscall.Close(recvFd)
		return nil, fmt.Errorf("udp recv socket failed: %v (need root)", err)
	}
	if err := syscall.Bind(recvUDPFd, addr); err != nil {
		syscall.Close(sendFd)
		syscall.Close(recvFd)
		syscall.Close(recvUDPFd)
		return nil, fmt.Errorf("udp bind failed: %v", err)
	}

	ep := &RawSocketEndpoint{
		sendFd:    sendFd,
		recvFd:    recvFd,
		recvUDPFd: recvUDPFd,
		nicID:     nicID,
	}
	fmt.Sscanf(getLocalIP(), "%d.%d.%d.%d", &ep.localIP[0], &ep.localIP[1], &ep.localIP[2], &ep.localIP[3])

	// One loop per socket: each has its own blocking Recvfrom, and there is nothing to share —
	// a select over raw sockets would add a third moving part where a second goroutine is enough.
	go ep.readLoop(ep.recvFd, 6)
	go ep.readLoop(ep.recvUDPFd, 17)
	return ep, nil
}

func (e *RawSocketEndpoint) SetTransportSender(sendFunc func([]byte)) {
	e.sendToTransport = sendFunc
}

// readLoop is the return path of ONE protocol: the socket is opened for it, and wantProto merely
// confirms that from the header (a raw socket brings nothing else, but the packet is parsed anyway).
//
// The minimum length is protocol-dependent: TCP's header is 20 bytes (40 with IP), UDP's is 8
// (28 with IP). A single threshold of 40 would drop short datagrams — a 30-byte DNS reply is
// exactly that, and the loss would look like "UDP doesn't work" even though the packet arrived.
func (e *RawSocketEndpoint) readLoop(fd int, wantProto byte) {
	buf := make([]byte, 65535)
	minLen := 40
	if wantProto == 17 {
		minLen = 28
	}

	for {
		n, _, err := syscall.Recvfrom(fd, buf, 0)
		if err != nil {
			if err == syscall.EAGAIN || err == syscall.EWOULDBLOCK {
				time.Sleep(10 * time.Millisecond)
				continue
			}
			utils.Debugf("[RAW-NIC%d/%d] Read error: %v", e.nicID, wantProto, err)
			return
		}
		if n < minLen {
			continue
		}

		protocol := buf[9]

		if protocol == wantProto && bytes.Equal(buf[16:20], e.localIP[:]) {
			ipHeaderLenIn := int(buf[0]&0x0F) * 4
			l4In := buf[ipHeaderLenIn:n]
			dstPort := uint16(l4In[2])<<8 | uint16(l4In[3])

			// A client port, not our own traffic. The gate is shared by both protocols: for TCP
			// the port is registered by the SYN, for UDP by the first outbound datagram
			// (see WritePackets).
			if _, active := e.activePorts.Load(dstPort); !active {
				continue
			}

			// A SYN-ACK is matched against our own SYN so that a reply to somebody else's
			// connection attempt on the same port never reaches the tunnel. UDP has no
			// connection, so there is nothing to match.
			if protocol == 6 && l4In[13] == 0x12 {
				ackNum := uint32(l4In[8])<<24 | uint32(l4In[9])<<16 | uint32(l4In[10])<<8 | uint32(l4In[11])
				synSeq := ackNum - 1

				if _, ok := e.outgoingSYNs.Load(synSeq); !ok {
					continue
				}
				e.outgoingSYNs.Delete(synSeq)
			}

			pktCopy := make([]byte, n)
			copy(pktCopy, buf[:n])

			copy(pktCopy[16:20], []byte{10, 10, 10, 2})

			pktCopy[10] = 0
			pktCopy[11] = 0
			ipChecksumVal := network.IPChecksum(pktCopy[:20])
			pktCopy[10] = byte(ipChecksumVal >> 8)
			pktCopy[11] = byte(ipChecksumVal & 0xFF)

			ipHeaderLen := int(pktCopy[0]&0x0F) * 4
			srcIPBytes := [4]byte{pktCopy[12], pktCopy[13], pktCopy[14], pktCopy[15]}
			dstIPBytes := [4]byte{pktCopy[16], pktCopy[17], pktCopy[18], pktCopy[19]}
			rewriteL4Checksum(pktCopy[ipHeaderLen:], protocol, srcIPBytes, dstIPBytes)

			if e.sendToTransport != nil {
				e.sendToTransport(pktCopy)
			}
		}
	}
}

func (e *RawSocketEndpoint) WritePackets(pkts stack.PacketBufferList) (int, tcpip.Error) {
	n := 0
	for _, pkt := range pkts.AsSlice() {
		ipPacket := pkt.ToView().ToSlice()
		// 28 = IP(20) + UDP(8): the threshold follows the SHORTEST protocol we carry. The former
		// 40 (IP+TCP) would silently drop the client's short datagrams.
		if len(ipPacket) < 28 {
			continue
		}

		pktCopy := make([]byte, len(ipPacket))
		copy(pktCopy, ipPacket)

		copy(pktCopy[12:16], e.localIP[:])

		pktCopy[10] = 0
		pktCopy[11] = 0
		ipChecksumVal := network.IPChecksum(pktCopy[:20])
		pktCopy[10] = byte(ipChecksumVal >> 8)
		pktCopy[11] = byte(ipChecksumVal & 0xFF)

		proto := pktCopy[9]
		ipHeaderLen := int(pktCopy[0]&0x0F) * 4
		l4 := pktCopy[ipHeaderLen:]
		srcIPBytes := [4]byte{pktCopy[12], pktCopy[13], pktCopy[14], pktCopy[15]}
		dstIPBytes := [4]byte{pktCopy[16], pktCopy[17], pktCopy[18], pktCopy[19]}
		rewriteL4Checksum(l4, proto, srcIPBytes, dstIPBytes)

		srcPort := uint16(l4[0])<<8 | uint16(l4[1])

		switch proto {
		case 6:
			if l4[13]&0x02 != 0 {
				seqNum := uint32(l4[4])<<24 | uint32(l4[5])<<16 | uint32(l4[6])<<8 | uint32(l4[7])
				e.outgoingSYNs.Store(seqNum, true)
				e.activePorts.Store(srcPort, true)
			}
			if l4[13]&0x01 != 0 || l4[13]&0x04 != 0 {
				dstPort := uint16(l4[2])<<8 | uint16(l4[3])
				e.activePorts.Delete(dstPort)
			}
		case 17:
			// UDP has neither a handshake nor FIN/RST: the very first datagram opens the port and
			// nothing closes it. The entry lives until the process exits — same as a TCP port
			// whose session was torn down without a FIN. No reaper here on purpose: a tunnel
			// session (a single client) sees tens of ports, and a lifetime timer would be a
			// guess — there is no way to know how long a quiet UDP flow stays interesting.
			e.activePorts.Store(srcPort, true)
		}

		var dst [4]byte
		copy(dst[:], pktCopy[16:20])

		addr := &syscall.SockaddrInet4{
			Addr: dst,
			Port: 0,
		}

		if err := syscall.Sendto(e.sendFd, pktCopy, 0, addr); err != nil {
			utils.Debugf("[RAW-NIC%d] Sendto failed: %v", e.nicID, err)
			continue
		}

		e.packetOut.Add(1)
		n++
	}
	return n, nil
}

func (e *RawSocketEndpoint) MTU() uint32                    { return 1500 }
func (e *RawSocketEndpoint) MaxHeaderLength() uint16        { return 0 }
func (e *RawSocketEndpoint) LinkAddress() tcpip.LinkAddress { return "" }
func (e *RawSocketEndpoint) Capabilities() stack.LinkEndpointCapabilities {
	return stack.CapabilityNone
}
func (e *RawSocketEndpoint) Attach(dispatcher stack.NetworkDispatcher) {
	e.dispatcher = dispatcher
}
func (e *RawSocketEndpoint) IsAttached() bool                        { return e.dispatcher != nil }
func (e *RawSocketEndpoint) Wait()                                   {}
func (e *RawSocketEndpoint) ARPHardwareType() header.ARPHardwareType { return header.ARPHardwareNone }
func (e *RawSocketEndpoint) AddHeader(*stack.PacketBuffer)           {}
func (e *RawSocketEndpoint) Close() {
	syscall.Close(e.sendFd)
	syscall.Close(e.recvFd)
	syscall.Close(e.recvUDPFd)
}
func (e *RawSocketEndpoint) SetMTU(uint32)                        {}
func (e *RawSocketEndpoint) SetLinkAddress(tcpip.LinkAddress)     {}
func (e *RawSocketEndpoint) ParseHeader(*stack.PacketBuffer) bool { return true }
func (e *RawSocketEndpoint) SetOnCloseAction(func())              {}
