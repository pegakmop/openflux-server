package gateway

import (
	"bufio"
	"crypto/tls"
	"fmt"
	"io"
	"net"
	"net/http"
	neturl "net/url"
	"strings"
	"time"
)

// dnsUpstreamKind selects how queryUpstream actually talks to dnsUpstream -
// see parseDNSUpstream for the three accepted forms.
type dnsUpstreamKind int

const (
	dnsUpstreamPlain dnsUpstreamKind = iota // classic DNS-over-TCP (today's only behavior)
	dnsUpstreamDoT                          // DNS-over-TLS, RFC 7858
	dnsUpstreamDoH                          // DNS-over-HTTPS, RFC 8484 (wireformat)
)

// dnsUpstreamConfig is dnsUpstream, parsed once at construction so relayDNS
// doesn't re-parse it on every query.
type dnsUpstreamConfig struct {
	kind dnsUpstreamKind
	addr string // host:port to dial via Server.dialer.DialTCP
	host string // TLS ServerName / HTTP Host header (DoT/DoH only)
	path string // HTTP request path (DoH only)
}

// parseDNSUpstream turns the user-supplied dnsUpstream string into a
// resolved config. Three forms are accepted, on both the client and the
// exit node (the same Server code runs as either) - a query through
// whichever of these ends up doing the actual dial rides the covert
// channel like everything else, so this only ever adds protocol privacy
// for the DNS exchange itself, not a way around the tunnel:
//
//   - "host" or "host:port" (default port 53): classic DNS-over-TCP -
//     unchanged from before this existed.
//   - "tls://host" or "tls://host:port" (default port 853): DNS-over-TLS.
//     Reuses writeDNSOverTCP/readDNSOverTCP as-is - RFC 7858 specifies the
//     exact same 2-byte length-prefixed framing as plain DNS-over-TCP,
//     just inside a TLS session.
//   - "https://host[:port][/path]" (default port 443, default path
//     /dns-query): DNS-over-HTTPS, the wireformat variant (a raw DNS
//     message as the POST body/response, not the JSON one).
//
// An https:// value that fails to parse as a URL at all falls back to
// dnsUpstreamPlain with the raw string as-is - dialing it will simply fail
// with a clear error rather than silently misinterpreting a typo.
func parseDNSUpstream(raw string) dnsUpstreamConfig {
	if raw == "" {
		raw = "77.88.8.8:53"
	}

	switch {
	case strings.HasPrefix(raw, "tls://"):
		host := strings.TrimPrefix(raw, "tls://")
		hostname := host
		if h, _, err := net.SplitHostPort(host); err == nil {
			hostname = h
		} else {
			host = net.JoinHostPort(host, "853")
		}
		return dnsUpstreamConfig{kind: dnsUpstreamDoT, addr: host, host: hostname}

	case strings.HasPrefix(raw, "https://"):
		u, err := neturl.Parse(raw)
		if err != nil {
			return dnsUpstreamConfig{kind: dnsUpstreamPlain, addr: raw}
		}
		addr := u.Host
		if _, _, err := net.SplitHostPort(addr); err != nil {
			addr = net.JoinHostPort(addr, "443")
		}
		path := u.Path
		if path == "" {
			path = "/dns-query"
		}
		return dnsUpstreamConfig{kind: dnsUpstreamDoH, addr: addr, host: u.Hostname(), path: path}

	default:
		addr := raw
		if _, _, err := net.SplitHostPort(addr); err != nil {
			addr = net.JoinHostPort(addr, "53")
		}
		return dnsUpstreamConfig{kind: dnsUpstreamPlain, addr: addr}
	}
}

// queryUpstream resolves query against s.dnsUpstreamCfg, dialing out through
// s.dialer either way (see parseDNSUpstream's doc comment on why DoT/DoH
// here is a privacy upgrade for the DNS exchange, not a tunnel bypass).
func (s *Server) queryUpstream(query []byte) ([]byte, error) {
	switch s.dnsUpstreamCfg.kind {
	case dnsUpstreamDoT:
		return s.queryDoT(query)
	case dnsUpstreamDoH:
		return s.queryDoH(query)
	default:
		return s.queryPlainDNS(query)
	}
}

func (s *Server) queryPlainDNS(query []byte) ([]byte, error) {
	conn, err := s.dialer.DialTCP(s.dnsUpstreamCfg.addr)
	if err != nil {
		return nil, fmt.Errorf("dial: %w", err)
	}
	defer conn.Close()
	conn.SetDeadline(time.Now().Add(dnsQueryTimeout))

	if err := writeDNSOverTCP(conn, query); err != nil {
		return nil, fmt.Errorf("write: %w", err)
	}
	return readDNSOverTCP(conn)
}

// dialUpstreamTLS dials dnsUpstreamCfg.addr and performs the TLS handshake -
// shared by DoT and DoH, which differ only in what goes over the connection
// afterward.
func (s *Server) dialUpstreamTLS() (*tls.Conn, error) {
	raw, err := s.dialer.DialTCP(s.dnsUpstreamCfg.addr)
	if err != nil {
		return nil, fmt.Errorf("dial: %w", err)
	}
	raw.SetDeadline(time.Now().Add(dnsQueryTimeout))

	conn := tls.Client(raw, s.dnsUpstreamTLSConfig)
	if err := conn.Handshake(); err != nil {
		raw.Close()
		return nil, fmt.Errorf("tls handshake: %w", err)
	}
	return conn, nil
}

func (s *Server) queryDoT(query []byte) ([]byte, error) {
	conn, err := s.dialUpstreamTLS()
	if err != nil {
		return nil, err
	}
	defer conn.Close()

	if err := writeDNSOverTCP(conn, query); err != nil {
		return nil, fmt.Errorf("write: %w", err)
	}
	return readDNSOverTCP(conn)
}

// queryDoH speaks RFC 8484's wireformat over a single, one-shot HTTP/1.1
// request - not a general HTTP client (no redirects, no connection reuse,
// no chunked-request handling), which this exchange has no use for: one
// query, one response, one connection, matching the existing plain-DNS and
// DoT paths' shape. http.ReadResponse parses the reply so this isn't
// hand-rolling HTTP framing itself, just the request line.
func (s *Server) queryDoH(query []byte) ([]byte, error) {
	if len(query) > maxDNSMessageSize {
		return nil, fmt.Errorf("dns message too large: %d bytes", len(query))
	}
	conn, err := s.dialUpstreamTLS()
	if err != nil {
		return nil, err
	}
	defer conn.Close()

	var req strings.Builder
	fmt.Fprintf(&req, "POST %s HTTP/1.1\r\n", s.dnsUpstreamCfg.path)
	fmt.Fprintf(&req, "Host: %s\r\n", s.dnsUpstreamCfg.host)
	req.WriteString("Content-Type: application/dns-message\r\n")
	req.WriteString("Accept: application/dns-message\r\n")
	fmt.Fprintf(&req, "Content-Length: %d\r\n", len(query))
	req.WriteString("Connection: close\r\n\r\n")

	if _, err := io.WriteString(conn, req.String()); err != nil {
		return nil, fmt.Errorf("write request: %w", err)
	}
	if _, err := conn.Write(query); err != nil {
		return nil, fmt.Errorf("write body: %w", err)
	}

	resp, err := http.ReadResponse(bufio.NewReader(conn), nil)
	if err != nil {
		return nil, fmt.Errorf("read response: %w", err)
	}
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusOK {
		return nil, fmt.Errorf("doh status %d", resp.StatusCode)
	}
	body, err := io.ReadAll(io.LimitReader(resp.Body, maxDNSMessageSize))
	if err != nil {
		return nil, fmt.Errorf("read body: %w", err)
	}
	return body, nil
}
