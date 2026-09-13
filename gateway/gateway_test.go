package gateway

import (
	"errors"
	"net"
	"testing"
	"time"
)

type fakeDialer struct{}

func (fakeDialer) DialTCP(string) (net.Conn, error) { return nil, errors.New("not implemented") }
func (fakeDialer) DialUDP(string) (net.Conn, error) { return nil, errors.New("not implemented") }

// funcDialer lets a test supply just the DialUDP behavior it needs.
type funcDialer struct {
	dialUDP func(string) (net.Conn, error)
}

func (f funcDialer) DialTCP(string) (net.Conn, error)      { return nil, errors.New("not implemented") }
func (f funcDialer) DialUDP(addr string) (net.Conn, error) { return f.dialUDP(addr) }

func TestNewServerNormalizesDNSUpstream(t *testing.T) {
	cases := []struct {
		name  string
		input string
		want  string
	}{
		{"empty defaults to yandex dns", "", "77.88.8.8:53"},
		{"bare host gets :53 appended", "9.9.9.9", "9.9.9.9:53"},
		{"host:port left untouched", "8.8.8.8:5353", "8.8.8.8:5353"},
	}

	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			s := NewServer(fakeDialer{}, c.input)
			if s.dnsUpstreamCfg.kind != dnsUpstreamPlain {
				t.Fatalf("kind = %v, want dnsUpstreamPlain", s.dnsUpstreamCfg.kind)
			}
			if s.dnsUpstreamCfg.addr != c.want {
				t.Errorf("addr = %q, want %q", s.dnsUpstreamCfg.addr, c.want)
			}
		})
	}
}

// --- relayUDP: exercises the generic (non-DNS) UDP relay path -----------

func TestRelayUDPBidirectional(t *testing.T) {
	// A real local UDP server standing in for whatever relayUDP dials via
	// Dialer.DialUDP - echoes each datagram back with a prefix so the test
	// can tell a reply actually round-tripped through the relay.
	echoConn, err := net.ListenUDP("udp", &net.UDPAddr{IP: net.IPv4(127, 0, 0, 1)})
	if err != nil {
		t.Fatalf("listen: %v", err)
	}
	defer echoConn.Close()

	go func() {
		buf := make([]byte, 1024)
		for {
			n, addr, err := echoConn.ReadFromUDP(buf)
			if err != nil {
				return
			}
			echoConn.WriteToUDP(append([]byte("echo:"), buf[:n]...), addr)
		}
	}()

	dialer := funcDialer{dialUDP: func(address string) (net.Conn, error) {
		return net.Dial("udp", address)
	}}

	appSide, gatewaySide := net.Pipe()
	s := &Server{dialer: dialer}
	go s.relayUDP(gatewaySide, echoConn.LocalAddr().String())
	defer appSide.Close()

	if _, err := appSide.Write([]byte("hello")); err != nil {
		t.Fatalf("write: %v", err)
	}
	appSide.SetReadDeadline(time.Now().Add(3 * time.Second))
	buf := make([]byte, 1024)
	n, err := appSide.Read(buf)
	if err != nil {
		t.Fatalf("read reply: %v", err)
	}
	if got := string(buf[:n]); got != "echo:hello" {
		t.Errorf("got %q, want %q", got, "echo:hello")
	}
}

func TestRelayUDPClosesLocalConnWhenDialFails(t *testing.T) {
	dialer := funcDialer{dialUDP: func(string) (net.Conn, error) {
		return nil, errors.New("dial refused")
	}}

	appSide, gatewaySide := net.Pipe()
	s := &Server{dialer: dialer}
	s.relayUDP(gatewaySide, "127.0.0.1:1") // synchronous: returns once it gives up

	appSide.SetReadDeadline(time.Now().Add(2 * time.Second))
	if _, err := appSide.Read(make([]byte, 1)); err == nil {
		t.Errorf("expected the local side to be closed after a failed dial")
	}
}
