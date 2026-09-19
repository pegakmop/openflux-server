package socks5

import (
	"errors"
	"io"
	"net"
	"sync"
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

type capturingDialer struct {
	mu   sync.Mutex
	addr string
}

func (d *capturingDialer) DialTCP(address string) (net.Conn, error) {
	d.mu.Lock()
	d.addr = address
	d.mu.Unlock()
	server, client := net.Pipe()
	go func() {
		buf := make([]byte, 64)
		n, _ := client.Read(buf)
		client.Write(buf[:n]) // echo, so the test can confirm payload bytes reached the target
	}()
	return server, nil
}

// A CONNECT request for a domain name, sent as several separate writes with pauses between them -
// real TCP has no message boundaries, so a request split across packets (routine on a mobile
// network) must still parse correctly instead of reading past what actually arrived.
func TestHandleConnectionAssemblesAFragmentedRequest(t *testing.T) {
	dialer := &capturingDialer{}
	s := NewSOCKS5Server("127.0.0.1:0", dialer)

	go s.Start()
	defer s.Stop()

	var addr string
	deadline := time.Now().Add(2 * time.Second)
	for {
		s.mu.Lock()
		l := s.listener
		s.mu.Unlock()
		if l != nil {
			addr = l.Addr().String()
			break
		}
		if time.Now().After(deadline) {
			t.Fatal("server never started listening")
		}
		time.Sleep(time.Millisecond)
	}

	conn, err := net.Dial("tcp", addr)
	if err != nil {
		t.Fatalf("dial: %v", err)
	}
	defer conn.Close()

	domain := "example.com"
	write := func(b []byte) {
		if _, err := conn.Write(b); err != nil {
			t.Fatalf("write: %v", err)
		}
		time.Sleep(20 * time.Millisecond) // give the server its own separate Read for this chunk
	}

	write([]byte{0x05, 0x01})             // greeting: VER, NMETHODS
	write([]byte{0x00})                   // METHODS[0] = no-auth
	write([]byte{0x05, 0x01, 0x00, 0x03}) // request header: VER, CMD=CONNECT, RSV, ATYP=domain
	write([]byte{byte(len(domain))})      // domain length
	write([]byte(domain))                 // domain, no port yet
	write([]byte{0x01, 0xBB})             // port 443, in its own write
	write([]byte("payload"))              // pipelined data right after the request

	buf := make([]byte, 2)
	if _, err := io.ReadFull(conn, buf); err != nil || buf[0] != 0x05 || buf[1] != 0x00 {
		t.Fatalf("auth reply = %v, err=%v, want [5 0]", buf, err)
	}
	reply := make([]byte, 10)
	if _, err := io.ReadFull(conn, reply); err != nil || reply[1] != 0x00 {
		t.Fatalf("connect reply = %v, err=%v, want success", reply, err)
	}

	echoed := make([]byte, len("payload"))
	conn.SetReadDeadline(time.Now().Add(2 * time.Second))
	if _, err := io.ReadFull(conn, echoed); err != nil {
		t.Fatalf("payload was never relayed to the target and echoed back: %v", err)
	}
	if string(echoed) != "payload" {
		t.Fatalf("echoed payload = %q, want %q", echoed, "payload")
	}

	dialer.mu.Lock()
	gotAddr := dialer.addr
	dialer.mu.Unlock()
	if gotAddr != "example.com:443" {
		t.Fatalf("DialTCP address = %q, want %q", gotAddr, "example.com:443")
	}
}
