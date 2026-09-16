package tunnel

import "testing"

// These exercise the shared-core demux/cleanup logic directly, without any
// real raw sockets (root, or even Linux networking, isn't needed for this
// part) - see rawSocketCore's doc comment for why the map itself, not the
// sockets, is what has to behave correctly here.

func TestRawSocketCoreActivePortsRoutesToOwningWorker(t *testing.T) {
	core := &rawSocketCore{}
	a := &RawSocketEndpoint{core: core}
	b := &RawSocketEndpoint{core: core}

	core.activePorts.Store(uint16(40000), a)
	core.activePorts.Store(uint16(40001), b)

	v, ok := core.activePorts.Load(uint16(40000))
	if !ok || v.(*RawSocketEndpoint) != a {
		t.Fatalf("port 40000 should route to worker a")
	}
	v, ok = core.activePorts.Load(uint16(40001))
	if !ok || v.(*RawSocketEndpoint) != b {
		t.Fatalf("port 40001 should route to worker b")
	}
}

// Close must remove only the calling worker's own port registrations - a
// shared core means another worker's entries living in the very same map
// must survive a sibling worker closing (this used to be implicit: each
// worker's own socket, and therefore its own map, simply stopped existing
// on Close).
func TestRawSocketEndpointCloseOnlySweepsItsOwnPorts(t *testing.T) {
	core := &rawSocketCore{}
	a := &RawSocketEndpoint{core: core}
	b := &RawSocketEndpoint{core: core}

	core.activePorts.Store(uint16(40000), a)
	core.activePorts.Store(uint16(40001), a)
	core.activePorts.Store(uint16(50000), b)

	a.Close()

	if _, ok := core.activePorts.Load(uint16(40000)); ok {
		t.Errorf("port 40000 (worker a) should have been removed by a.Close()")
	}
	if _, ok := core.activePorts.Load(uint16(40001)); ok {
		t.Errorf("port 40001 (worker a) should have been removed by a.Close()")
	}
	if v, ok := core.activePorts.Load(uint16(50000)); !ok || v.(*RawSocketEndpoint) != b {
		t.Errorf("port 50000 (worker b) should survive a.Close(), got ok=%v", ok)
	}
}
