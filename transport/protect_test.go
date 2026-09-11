package transport

import (
	"net"
	"strings"
	"testing"
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
