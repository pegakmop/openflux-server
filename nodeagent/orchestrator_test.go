package nodeagent

import "testing"

// --- portAllocator --------------------------------------------------------

// TestPortAllocatorRangesNeverOverlap guards the actual production bug this
// allocator exists to prevent: two keys' gvisor stacks independently
// choosing the same source port and cross-delivering each other's traffic
// (see TCPTunnel.SetPortRange's doc comment). Every concurrently-held range
// must be disjoint from every other one.
func TestPortAllocatorRangesNeverOverlap(t *testing.T) {
	var p portAllocator

	type held struct{ start, end uint16 }
	var ranges []held

	for i := 0; i < 20; i++ {
		_, start, end, ok := p.alloc()
		if !ok {
			t.Fatalf("alloc() #%d failed unexpectedly", i)
		}
		for _, r := range ranges {
			if start <= r.end && r.start <= end {
				t.Fatalf("range [%d,%d] overlaps already-held range [%d,%d]", start, end, r.start, r.end)
			}
		}
		ranges = append(ranges, held{start, end})
	}
}

// TestPortAllocatorRecyclesReleasedRanges guards against a leak: an
// allocator that only ever grows would exhaust the port space long before
// a node's real key churn (keys added and removed over time) should matter.
func TestPortAllocatorRecyclesReleasedRanges(t *testing.T) {
	var p portAllocator

	idx1, start1, end1, ok := p.alloc()
	if !ok {
		t.Fatalf("alloc() #1 failed unexpectedly")
	}
	p.release(idx1)

	idx2, start2, end2, ok := p.alloc()
	if !ok {
		t.Fatalf("alloc() #2 failed unexpectedly")
	}

	if idx2 != idx1 || start2 != start1 || end2 != end1 {
		t.Errorf("alloc() after release = (idx=%d, %d-%d), want the recycled (idx=%d, %d-%d)",
			idx2, start2, end2, idx1, start1, end1)
	}
}

// TestPortAllocatorExhaustion guards the failure path: once the port space
// is used up, alloc must report that cleanly rather than handing out a
// range past portRangeMax or overlapping one already held.
func TestPortAllocatorExhaustion(t *testing.T) {
	var p portAllocator

	maxWorkers := (portRangeMax - portRangeBase + 1) / portRangeSize
	for i := 0; i < maxWorkers; i++ {
		if _, _, _, ok := p.alloc(); !ok {
			t.Fatalf("alloc() #%d failed before capacity should be exhausted (capacity=%d)", i, maxWorkers)
		}
	}

	if _, _, _, ok := p.alloc(); ok {
		t.Fatalf("alloc() succeeded past this node's port capacity (%d workers)", maxWorkers)
	}
}

func TestDiffCounter(t *testing.T) {
	cases := []struct {
		name              string
		previous, current uint64
		want              uint64
	}{
		{"normal growth", 100, 150, 50},
		{"no change", 100, 100, 0},
		{"reconnect reset treated as count-from-zero", 500, 20, 20},
	}

	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			if got := diffCounter(c.previous, c.current); got != c.want {
				t.Errorf("diffCounter(%d, %d) = %d, want %d", c.previous, c.current, got, c.want)
			}
		})
	}
}
