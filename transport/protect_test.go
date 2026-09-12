package transport

import (
	"context"
	"net"
	"strings"
	"testing"
	"time"
)

func TestProtectedDialerCallsProtectorWithFD(t *testing.T) {
	ln, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatalf("listen: %v", err)
	}
	defer ln.Close()

	var gotFD int
	SetProtector(func(fd int) bool {
		gotFD = fd
		return true
	})
	t.Cleanup(func() { SetProtector(nil) })

	conn, err := ProtectedDialer().Dial("tcp", ln.Addr().String())
	if err != nil {
		t.Fatalf("dial: %v", err)
	}
	conn.Close()

	if gotFD <= 0 {
		t.Errorf("protector fd = %d, want a real positive fd", gotFD)
	}
}

func TestProtectedDialerFailsWhenProtectorRejects(t *testing.T) {
	ln, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatalf("listen: %v", err)
	}
	defer ln.Close()

	SetProtector(func(fd int) bool { return false })
	t.Cleanup(func() { SetProtector(nil) })

	_, err = ProtectedDialer().Dial("tcp", ln.Addr().String())
	if err == nil {
		t.Fatalf("expected an error when the protector rejects the socket")
	}
	if !strings.Contains(err.Error(), "protect") {
		t.Errorf("error = %q, want it to mention protection failing", err)
	}
}

// TestProtectedResolverIgnoresRequestedAddress guards the actual Android bug:
// Go's resolver asks Dial to connect to whatever nameserver it thinks it
// found (127.0.0.1:53 on Android, since there's no /etc/resolv.conf to read
// - see ProtectedResolver's doc comment), and nothing listens there. If this
// regresses to actually dialing the requested address, this test hangs/errors
// instead of connecting to the fake bootstrap server.
func TestProtectedResolverIgnoresRequestedAddress(t *testing.T) {
	ln, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatalf("listen: %v", err)
	}
	defer ln.Close()

	original := bootstrapDNSServers
	bootstrapDNSServers = []string{ln.Addr().String()}
	t.Cleanup(func() { bootstrapDNSServers = original })

	accepted := make(chan struct{}, 1)
	go func() {
		conn, err := ln.Accept()
		if err != nil {
			return
		}
		accepted <- struct{}{}
		conn.Close()
	}()

	// A nameserver address nothing is listening on - simulates Android's
	// broken defaultNS fallback. The resolver must not actually use it.
	conn, err := ProtectedResolver().Dial(context.Background(), "tcp", "127.0.0.1:1")
	if err != nil {
		t.Fatalf("Dial: %v", err)
	}
	conn.Close()

	select {
	case <-accepted:
	case <-time.After(2 * time.Second):
		t.Fatalf("bootstrap server was never dialed - Dial used the (broken) requested address instead")
	}
}

func TestProtectedDialerNoProtectorIsANoop(t *testing.T) {
	ln, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatalf("listen: %v", err)
	}
	defer ln.Close()

	SetProtector(nil)

	conn, err := ProtectedDialer().Dial("tcp", ln.Addr().String())
	if err != nil {
		t.Fatalf("dial with no protector registered: %v", err)
	}
	conn.Close()
}
