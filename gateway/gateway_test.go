package gateway

import (
	"errors"
	"net"
	"testing"
)

type fakeDialer struct{}

func (fakeDialer) DialTCP(string) (net.Conn, error) { return nil, errors.New("not implemented") }

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
			if s.dnsUpstream != c.want {
				t.Errorf("dnsUpstream = %q, want %q", s.dnsUpstream, c.want)
			}
		})
	}
}
