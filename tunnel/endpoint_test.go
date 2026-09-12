package tunnel

import (
	"testing"

	"gvisor.dev/gvisor/pkg/tcpip"
	"gvisor.dev/gvisor/pkg/tcpip/stack"
)

func TestInjectInboundBeforeAttachDoesNotPanic(t *testing.T) {
	// A packet can arrive (or be mid-flight from a Close() racing a
	// send) before/after this endpoint has a dispatcher - see
	// InjectInbound's doc comment. Must be silently dropped, not crash.
	e := NewTunnelLinkEndpoint()
	e.InjectInbound([]byte{0x45, 0x00, 0x00, 0x14})

	if in, _ := e.PacketCounts(); in != 1 {
		t.Errorf("PacketCounts().in = %d, want 1 (still counted, just not dispatched)", in)
	}
}

func TestAttachNilDetachesCleanly(t *testing.T) {
	e := NewTunnelLinkEndpoint()
	e.Attach(fakeDispatcher{})
	if !e.IsAttached() {
		t.Fatalf("expected IsAttached() after Attach(non-nil)")
	}

	e.Attach(nil)
	if e.IsAttached() {
		t.Errorf("expected !IsAttached() after Attach(nil)")
	}

	// Must not panic even though a dispatcher was attached a moment ago.
	e.InjectInbound([]byte{0x45, 0x00, 0x00, 0x14})
}

func TestPacketCounts(t *testing.T) {
	e := NewTunnelLinkEndpoint()
	if in, out := e.PacketCounts(); in != 0 || out != 0 {
		t.Fatalf("PacketCounts() on a fresh endpoint = (%d, %d), want (0, 0)", in, out)
	}

	e.InjectInbound([]byte{0x45, 0x00})
	e.InjectInbound([]byte{0x45, 0x00})
	if in, _ := e.PacketCounts(); in != 2 {
		t.Errorf("PacketCounts().in = %d, want 2", in)
	}
}

// TestInjectInboundRecoversFromDispatcherPanic guards a real production
// incident: gvisor's own NAT/conntrack code (enabled on the exit node via
// SetForwardingDefaultAndAllNICs) panics instead of returning an error on
// some packets it doesn't expect ("unexpected transport protocol = 0", seen
// live). InjectInbound runs on a transport's shared read-loop goroutine, so
// an unrecovered panic here took the whole exit-node process down -
// disconnecting every client it was serving, not just whoever sent the one
// packet that triggered it.
func TestInjectInboundRecoversFromDispatcherPanic(t *testing.T) {
	e := NewTunnelLinkEndpoint()
	e.Attach(panickingDispatcher{})

	e.InjectInbound([]byte{0x45, 0x00, 0x00, 0x14}) // must not panic

	if in, _ := e.PacketCounts(); in != 1 {
		t.Errorf("PacketCounts().in = %d, want 1", in)
	}
}

// fakeDispatcher is just enough of stack.NetworkDispatcher to prove
// Attach/IsAttached track a real (non-nil) value - InjectInbound's actual
// delivery through a live dispatcher is exercised via gvisor itself
// elsewhere (tunnel.TCPTunnel/gateway.Server), not worth re-deriving gvisor's
// own PacketBuffer/NetworkDispatcher plumbing just for this.
type fakeDispatcher struct{}

func (fakeDispatcher) DeliverNetworkPacket(tcpip.NetworkProtocolNumber, *stack.PacketBuffer) {}
func (fakeDispatcher) DeliverLinkPacket(tcpip.NetworkProtocolNumber, *stack.PacketBuffer)    {}

type panickingDispatcher struct{}

func (panickingDispatcher) DeliverNetworkPacket(tcpip.NetworkProtocolNumber, *stack.PacketBuffer) {
	panic("unexpected transport protocol = 0")
}
func (panickingDispatcher) DeliverLinkPacket(tcpip.NetworkProtocolNumber, *stack.PacketBuffer) {}
