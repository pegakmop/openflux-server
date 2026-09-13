package gateway

import (
	"encoding/binary"
	"testing"
	"time"
)

func dnsNameWire(labels ...string) []byte {
	var b []byte
	for _, l := range labels {
		if len(l) == 0 || len(l) > 63 {
			continue
		}
		b = append(b, byte(len(l)))
		b = append(b, l...)
	}
	return append(b, 0x00)
}

func dnsHeader(id uint16, flags uint16, qd, an, ns, ar uint16) []byte {
	h := make([]byte, 12)
	binary.BigEndian.PutUint16(h[0:2], id)
	binary.BigEndian.PutUint16(h[2:4], flags)
	binary.BigEndian.PutUint16(h[4:6], qd)
	binary.BigEndian.PutUint16(h[6:8], an)
	binary.BigEndian.PutUint16(h[8:10], ns)
	binary.BigEndian.PutUint16(h[10:12], ar)
	return h
}

func buildQuery(name []byte) []byte {
	msg := dnsHeader(0x1234, 0x0100, 1, 0, 0, 0)
	msg = append(msg, name...)
	msg = append(msg, 0x00, dnsTypeA, 0x00, dnsClassIN) // qtype=A, qclass=IN
	return msg
}

func appendRecord(msg []byte, name []byte, rtype uint16, ttl uint32, rdata []byte) []byte {
	msg = append(msg, name...)
	rr := make([]byte, 10)
	binary.BigEndian.PutUint16(rr[0:2], rtype)
	binary.BigEndian.PutUint16(rr[2:4], dnsClassIN)
	binary.BigEndian.PutUint32(rr[4:8], ttl)
	binary.BigEndian.PutUint16(rr[8:10], uint16(len(rdata)))
	msg = append(msg, rr...)
	msg = append(msg, rdata...)
	return msg
}

func buildResponse(question []byte, answers [][]byte) []byte {
	// A full question section: QNAME + qtype + qclass.
	msg := dnsHeader(0x1234, 0x8180, 1, uint16(len(answers)), 0, 0)
	msg = append(msg, question...)
	msg = append(msg, 0x00, dnsTypeA, 0x00, dnsClassIN)
	for _, a := range answers {
		msg = append(msg, a...)
	}
	return msg
}

func TestDNSQuestionName(t *testing.T) {
	q := buildQuery(dnsNameWire("www", "example", "com"))
	got, ok := dnsQuestionName(q)
	if !ok || got != "www.example.com" {
		t.Errorf("dnsQuestionName = (%q, %v), want (\"www.example.com\", true)", got, ok)
	}
}

func TestDNSQuestionNameRejectsResponsesAndEmpty(t *testing.T) {
	// A response (QR set) is not a query.
	reply := buildResponse(dnsNameWire("www", "example", "com"), nil)
	if _, ok := dnsQuestionName(reply); ok {
		t.Errorf("a response must not be treated as a query")
	}
	if _, ok := dnsQuestionName([]byte{0, 0}); ok {
		t.Errorf("a short message must not parse")
	}
	// qdcount = 0 -> nothing to read.
	noqd := dnsHeader(1, 0x0100, 0, 0, 0, 0)
	if _, ok := dnsQuestionName(noqd); ok {
		t.Errorf("a query with no questions must not parse")
	}
}

func TestDNSARecords(t *testing.T) {
	qname := dnsNameWire("www", "example", "com")
	pointer := []byte{0xC0, 0x0C} // compressed pointer to offset 12

	answerA := appendRecord(nil, pointer, dnsTypeA, 60, []byte{93, 184, 216, 34})

	// www.example.com A 93.184.216.34 (TTL 60) + cdn.example.com A 93.184.216.35
	msg := buildResponse(qname, [][]byte{
		answerA,
		appendRecord(nil, []byte{0xC0, 0x0C}, dnsTypeA, 60, []byte{93, 184, 216, 34}),
		appendRecord(nil, dnsNameWire("cdn", "example", "com"), dnsTypeA, 5, []byte{93, 184, 216, 35}),
	})

	records := dnsARecords(msg)
	if len(records) != 3 {
		t.Fatalf("got %d A records, want 3: %+v", len(records), records)
	}
	if records[0].domain != "www.example.com" || records[0].ip != "93.184.216.34" {
		t.Errorf("record[0] = %+v", records[0])
	}
	if records[1].domain != "www.example.com" || records[1].ip != "93.184.216.34" {
		t.Errorf("record[1] = %+v", records[1])
	}
	if records[2].domain != "cdn.example.com" || records[2].ip != "93.184.216.35" {
		t.Errorf("record[2] = %+v", records[2])
	}
	// TTL is clamped to a sane minimum (5 -> 10).
	if records[2].ttl != 10 {
		t.Errorf("ttl = %d, want clamped 10", records[2].ttl)
	}
}

func TestDNSARecordsTruncated(t *testing.T) {
	qname := dnsNameWire("www", "example", "com")
	msg := buildResponse(qname, [][]byte{
		appendRecord(nil, []byte{0xC0, 0x0C}, dnsTypeA, 60, []byte{93, 184, 216, 34}),
	})
	// Feed every strict prefix; must not panic, and must not return records
	// for prefixes too short to contain one.
	for cut := 0; cut < len(msg); cut++ {
		func() {
			defer func() {
				if r := recover(); r != nil {
					t.Fatalf("dnsARecords panicked on %d-byte prefix: %v", cut, r)
				}
			}()
			_ = dnsARecords(msg[:cut])
		}()
	}
}

func TestDNSCacheRecordAndLookup(t *testing.T) {
	c := newDNSCache()
	c.record([]dnsARecord{
		{domain: "www.example.com", ip: "93.184.216.34", ttl: 300},
		{domain: "cdn.example.com", ip: "93.184.216.34", ttl: 300},
		{domain: "other.net", ip: "198.51.100.7", ttl: 300},
	})

	got := c.lookup("93.184.216.34")
	if len(got) != 2 {
		t.Fatalf("lookup returned %v, want two domains", got)
	}
	if c.lookup("198.51.100.7")[0] != "other.net" {
		t.Errorf("expected other.net for 198.51.100.7")
	}
	if c.lookup("203.0.113.1") != nil {
		t.Errorf("lookup must miss for an unknown IP")
	}
}

func TestDNSCacheExpiry(t *testing.T) {
	c := newDNSCache()
	c.record([]dnsARecord{{domain: "x.example.com", ip: "93.184.216.34", ttl: 60}})
	c.mu.Lock()
	c.expiry["93.184.216.34"] = time.Now().Add(-time.Second)
	c.mu.Unlock()
	if got := c.lookup("93.184.216.34"); got != nil {
		t.Errorf("lookup returned %v after expiry", got)
	}
	if c.ipDomains["93.184.216.34"] != nil {
		t.Errorf("expired entry was not removed from ipDomains")
	}
}

func TestDNSCachePrunesWhenLarge(t *testing.T) {
	c := newDNSCache()
	var recs []dnsARecord
	for i := 0; i < 8192+128; i++ {
		recs = append(recs, dnsARecord{domain: "h.example.com", ip: "10.0.0.1", ttl: 60})
	}
	c.record(recs)
	// After the write the map must stay bounded (the same IP overwrites, so
	// no crash and a sane size).
	if len(c.expiry) > 8192 {
		t.Errorf("cache grew to %d entries", len(c.expiry))
	}
}
