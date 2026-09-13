package gateway

import (
	"sync"
	"time"
)

// dnsCache remembers, for each resolved IPv4 address, which hostnames it
// answered for, so the gateway can classify SNI-less TCP connections by
// their destination IP against the site policy. Entries carry an expiry from
// the DNS reply's TTL (bounded by dnsMinTTLEntry/dnsMaxTTLEntry) and are
// pruned lazily when the map grows, so a long-lived tunnel never leaks
// memory or classifies an old IP with a fresh resolution.
type dnsCache struct {
	mu        sync.Mutex
	ipDomains map[string]map[string]struct{}
	expiry    map[string]time.Time
}

func newDNSCache() *dnsCache {
	return &dnsCache{
		ipDomains: make(map[string]map[string]struct{}),
		expiry:    make(map[string]time.Time),
	}
}

// record associates each A-record IP with its owner domain until expiry.
func (c *dnsCache) record(records []dnsARecord) {
	if c == nil || len(records) == 0 {
		return
	}
	c.mu.Lock()
	defer c.mu.Unlock()
	for _, r := range records {
		if r.domain == "" || r.domain == "." || r.ip == "" {
			continue
		}
		if c.ipDomains[r.ip] == nil {
			c.ipDomains[r.ip] = make(map[string]struct{})
		}
		c.ipDomains[r.ip][r.domain] = struct{}{}
		c.expiry[r.ip] = time.Now().Add(time.Duration(r.ttl) * time.Second)
	}
	c.pruneLocked()
}

// lookup returns the domains cached for ip, dropping any that have expired
// along the way.
func (c *dnsCache) lookup(ip string) []string {
	if c == nil {
		return nil
	}
	c.mu.Lock()
	defer c.mu.Unlock()
	if expiry, ok := c.expiry[ip]; ok && time.Now().After(expiry) {
		delete(c.ipDomains, ip)
		delete(c.expiry, ip)
		return nil
	}
	domains := c.ipDomains[ip]
	if len(domains) == 0 {
		return nil
	}
	out := make([]string, 0, len(domains))
	for d := range domains {
		out = append(out, d)
	}
	return out
}

// pruneLocked removes expired entries once the cache outgrows a fixed size,
// keeping it from growing unbounded on busy devices.
func (c *dnsCache) pruneLocked() {
	if len(c.expiry) <= 8192 {
		return
	}
	now := time.Now()
	for ip, expiry := range c.expiry {
		if now.After(expiry) {
			delete(c.ipDomains, ip)
			delete(c.expiry, ip)
		}
	}
}
