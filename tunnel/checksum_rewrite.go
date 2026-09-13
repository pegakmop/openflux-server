package tunnel

import "universal-bypass-tool/network"

// rewriteL4Checksum recomputes the transport checksum AFTER the addresses were rewritten.
//
// Deliberately in an OS-agnostic file: both halves of the exit node's NAT (outbound WritePackets
// and the return path in readLoop) and both raw-socket implementations (linux/darwin) must compute
// it the same way. A per-file copy would drift on the first edit, and a packet with a wrong
// checksum is dropped silently by the receiver — so the drift would surface far from its cause.
//
// The field offset differs: 16-17 for TCP, 6-7 for UDP. Protocols we don't know are left alone —
// corrupting someone else's checksum is worse than leaving it as it is.
func rewriteL4Checksum(l4 []byte, proto byte, srcIP, dstIP [4]byte) {
	switch proto {
	case 6:
		if len(l4) < 18 {
			return
		}
		l4[16], l4[17] = 0, 0
		sum := network.TCPChecksum(l4, srcIP, dstIP)
		l4[16], l4[17] = byte(sum>>8), byte(sum&0xFF)
	case 17:
		if len(l4) < 8 {
			return
		}
		l4[6], l4[7] = 0, 0
		sum := network.UDPChecksum(l4, srcIP, dstIP)
		// A zero UDP checksum means "not computed", so a computed zero is transmitted as 0xFFFF
		// (RFC 768) — otherwise the receiver concludes there is nothing to verify.
		if sum == 0 {
			sum = 0xFFFF
		}
		l4[6], l4[7] = byte(sum>>8), byte(sum&0xFF)
	}
}
