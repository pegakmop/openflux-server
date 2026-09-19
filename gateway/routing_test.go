package gateway

import (
	"net"
	"testing"
	"time"
)

func newTestServer(policy *SitePolicy) *Server {
	s := NewServerWithPolicy(fakeDialer{}, "77.88.8.8:53", policy)
	return s
}

func TestShouldBypassConnectionBySNI(t *testing.T) {
	include := NewSitePolicy(SiteSplitInclude, []string{"tunneled.example"})
	exclude := NewSitePolicy(SiteSplitExclude, []string{"direct.example"})

	sniTunneled := buildClientHello("app.tunneled.example", true)
	sniDirect := buildClientHello("app.direct.example", true)

	inc := newTestServer(include)
	exc := newTestServer(exclude)

	if inc.shouldBypassConnection(sniTunneled, "93.184.216.1:443") {
		t.Errorf("include: a listed domain must stay tunneled")
	}
	if !inc.shouldBypassConnection(sniDirect, "93.184.216.1:443") {
		t.Errorf("include: an unlisted domain must bypass")
	}
	if exc.shouldBypassConnection(sniTunneled, "93.184.216.1:443") {
		t.Errorf("exclude: an unlisted domain must stay tunneled")
	}
	if !exc.shouldBypassConnection(sniDirect, "93.184.216.1:443") {
		t.Errorf("exclude: an excluded domain must bypass")
	}
}

func TestShouldBypassConnectionByDNSCache(t *testing.T) {
	include := newTestServer(NewSitePolicy(SiteSplitInclude, []string{"tunneled.example"}))
	include.dnsCache.record([]dnsARecord{{domain: "app.tunneled.example", ip: "93.184.216.34", ttl: 300}})

	if include.shouldBypassConnection(nil, "93.184.216.34:443") {
		t.Errorf("SNI-less connection to a cached, tunneled site must stay tunneled")
	}

	include.dnsCache.record([]dnsARecord{{domain: "other.example", ip: "93.184.216.34", ttl: 300}})
	if include.shouldBypassConnection(nil, "93.184.216.34:443") {
		t.Errorf("mixed domains on a shared IP must stay tunneled")
	}

	exclude := newTestServer(NewSitePolicy(SiteSplitExclude, []string{"direct.example"}))
	exclude.dnsCache.record([]dnsARecord{{domain: "app.direct.example", ip: "198.51.100.7", ttl: 300}})
	if !exclude.shouldBypassConnection(nil, "198.51.100.7:443") {
		t.Errorf("SNI-less connection to a cached, excluded site must bypass")
	}
}

func TestShouldBypassConnectionUnknownDefaults(t *testing.T) {
	inc := newTestServer(NewSitePolicy(SiteSplitInclude, []string{"tunneled.example"}))
	if !inc.shouldBypassConnection(nil, "198.51.100.7:443") {
		t.Errorf("include: an unknown destination must default to direct")
	}
	exc := newTestServer(NewSitePolicy(SiteSplitExclude, []string{"direct.example"}))
	if exc.shouldBypassConnection(nil, "198.51.100.7:443") {
		t.Errorf("exclude: an unknown destination must default to tunneled")
	}
}

type stubResolver struct {
	conn   *net.UDPConn
	answer []byte
}

func newStubResolver(t *testing.T, records []dnsARecord) string {
	t.Helper()
	conn, err := net.ListenUDP("udp", &net.UDPAddr{IP: net.IPv4(127, 0, 0, 1)})
	if err != nil {
		t.Fatalf("stub resolver listen: %v", err)
	}
	sr := &stubResolver{conn: conn}
	go sr.serve(records)
	t.Cleanup(func() { conn.Close() })
	return conn.LocalAddr().String()
}

func (sr *stubResolver) serve(records []dnsARecord) {
	buf := make([]byte, 512)
	for {
		n, addr, err := sr.conn.ReadFromUDP(buf)
		if err != nil {
			return
		}
		reply := sr.buildReply(buf[:n], records)
		sr.conn.WriteToUDP(reply, addr)
	}
}

func (sr *stubResolver) buildReply(query []byte, records []dnsARecord) []byte {
	reply := dnsHeader(1, 0x8180, 1, uint16(len(records)), 0, 0)
	reply = append(reply, query[12:]...) // echo the question section
	var rdata [4]byte
	for _, r := range records {
		ip := net.ParseIP(r.ip).To4()
		copy(rdata[:], ip)
		reply = appendRecord(reply, []byte{0xC0, 0x0C}, dnsTypeA, r.ttl, rdata[:])
	}
	return reply
}

func TestResolveDirect(t *testing.T) {
	records := []dnsARecord{{domain: "bypass.example", ip: "93.184.216.34", ttl: 60}}
	s := newTestServer(nil)
	s.directResolvers = []string{newStubResolver(t, records)}

	query := buildQuery(dnsNameWire("bypass", "example"))
	reply := s.resolveDirect(query)
	if reply == nil {
		t.Fatalf("resolveDirect returned nothing")
	}
	got := dnsARecords(reply)
	if len(got) != 1 || got[0].ip != "93.184.216.34" || got[0].domain != "bypass.example" {
		t.Errorf("resolveDirect reply = %+v", got)
	}
}

func TestResolveDirectFallsBackThroughResolvers(t *testing.T) {
	records := []dnsARecord{{domain: "fallback.example", ip: "198.51.100.7", ttl: 60}}
	s := newTestServer(nil)
	// First resolver is a dead port; the second one answers.
	s.directResolvers = []string{"127.0.0.1:1", newStubResolver(t, records)}

	query := buildQuery(dnsNameWire("fallback", "example"))
	reply := s.resolveDirect(query)
	if reply == nil {
		t.Fatalf("resolveDirect returned nothing")
	}
	if got := dnsARecords(reply); len(got) != 1 || got[0].domain != "fallback.example" {
		t.Errorf("unexpected reply: %+v", got)
	}
}

func TestRelayDNSBypassesWhenRuleMatches(t *testing.T) {
	records := []dnsARecord{{domain: "bypass.example", ip: "93.184.216.34", ttl: 60}}
	s := newTestServer(NewSitePolicy(SiteSplitExclude, []string{"bypass.example"}))
	s.directResolvers = []string{newStubResolver(t, records)}

	query := buildQuery(dnsNameWire("bypass", "example"))

	app, gw := net.Pipe()
	done := make(chan []byte, 1)
	go func() {
		defer app.Close()
		_, err := app.Write(query)
		if err != nil {
			return
		}
		reply := make([]byte, 512)
		n, _ := app.Read(reply)
		done <- reply[:n]
	}()

	s.relayDNS(gw) // synchronous
	gw.Close()

	select {
	case reply := <-done:
		if got := dnsARecords(reply); len(got) != 1 || got[0].domain != "bypass.example" {
			t.Errorf("reply records = %+v", got)
		}
	case <-time.After(5 * time.Second):
		t.Fatalf("relayDNS did not reply")
	}
}
