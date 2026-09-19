package socks5

import (
	"errors"
	"net"
	"testing"
	"time"
)

type fakeDialer struct{}

func (fakeDialer) DialTCP(address string) (net.Conn, error) {
	return nil, errors.New("not implemented")
}

func TestSOCKS5ServerStopUnblocksStart(t *testing.T) {
	s := NewSOCKS5Server("127.0.0.1:0", fakeDialer{})

	done := make(chan error, 1)
	go func() { done <- s.Start() }()

	deadline := time.Now().Add(2 * time.Second)
	for {
		s.mu.Lock()
		ready := s.listener != nil
		s.mu.Unlock()
		if ready {
			break
		}
		if time.Now().After(deadline) {
			t.Fatal("server never started listening")
		}
		time.Sleep(time.Millisecond)
	}

	if err := s.Stop(); err != nil {
		t.Fatalf("Stop: %v", err)
	}

	select {
	case err := <-done:
		if err != nil {
			t.Fatalf("Start returned %v after Stop, want nil (clean shutdown)", err)
		}
	case <-time.After(2 * time.Second):
		t.Fatal("Start did not return after Stop")
	}
}

// Stop is safe to call before Start ever runs, or more than once.
func TestSOCKS5ServerStopBeforeStartIsANoop(t *testing.T) {
	s := NewSOCKS5Server("127.0.0.1:0", fakeDialer{})
	if err := s.Stop(); err != nil {
		t.Fatalf("Stop before Start: %v", err)
	}
	if err := s.Stop(); err != nil {
		t.Fatalf("second Stop: %v", err)
	}
}
