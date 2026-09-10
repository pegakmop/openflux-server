package api

import "testing"

func TestIPRateLimiterBurstThenBlocks(t *testing.T) {
	l := newIPRateLimiter(1, 3)

	for i := 0; i < 3; i++ {
		if !l.allow("1.2.3.4") {
			t.Fatalf("request %d within burst should be allowed", i)
		}
	}
	if l.allow("1.2.3.4") {
		t.Fatalf("request beyond burst should be rate limited")
	}
}

func TestIPRateLimiterTracksIndependently(t *testing.T) {
	l := newIPRateLimiter(1, 1)

	if !l.allow("1.1.1.1") {
		t.Fatalf("first request from 1.1.1.1 should be allowed")
	}
	if !l.allow("2.2.2.2") {
		t.Fatalf("first request from a different IP should be allowed independently")
	}
	if l.allow("1.1.1.1") {
		t.Fatalf("second immediate request from 1.1.1.1 should be limited")
	}
}
