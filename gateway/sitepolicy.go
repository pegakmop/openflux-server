package gateway

import (
	"net"
	"strings"
)

// SiteSplitMode controls how the per-site rules in a SitePolicy are applied -
// the site-level counterpart to Android's per-app split tunneling.
type SiteSplitMode int

const (
	// SiteSplitOff disables site-based routing: everything goes through the
	// tunnel (the pre-existing behavior).
	SiteSplitOff SiteSplitMode = iota
	// SiteSplitExclude keeps the tunnel for everything except the listed
	// sites, which bypass it and connect directly.
	SiteSplitExclude
	// SiteSplitInclude routes only the listed sites through the tunnel; every
	// other site connects directly.
	SiteSplitInclude
)

// ParseSiteSplitMode converts the wire string from mobile.Config. Unknown or
// empty values fall back to SiteSplitOff, mirroring how the app's
// SettingsRepository defaults its DataStore reads.
func ParseSiteSplitMode(name string) SiteSplitMode {
	switch name {
	case "exclude":
		return SiteSplitExclude
	case "include":
		return SiteSplitInclude
	default:
		return SiteSplitOff
	}
}

// SitePolicy decides, for a destination, whether its traffic must bypass the
// tunnel. Two kinds of entries are accepted and coexist in one list:
//
//   - domains, normalized on construction: trimmed, lowercased, and stripped
//     of a leading "*." or "." so "example.com", "*.example.com" and
//     ".example.com" all mean the same thing: that domain and everything under
//     it (sub.example.com, ...). A suffix wildcard like "*.ru" therefore
//     matches every site whose name ends in ".ru".
//   - IP addresses (IPv4 or IPv6, optionally wrapped in brackets), matched
//     against the literal destination address - handy for sites with no SNI
//     or traffic that never passes the DNS cache.
type SitePolicy struct {
	mode SiteSplitMode
	// sites holds normalized domains. For SiteSplitExclude they are the
	// bypass list; for SiteSplitInclude they are the only-tunneled list.
	sites []string
	// ips holds literal destination addresses from the list.
	ips map[string]struct{}
}

// NewSitePolicy builds a policy from a mode and a raw user-supplied site
// list, normalizing each entry. Repeated or redundant entries are dropped,
// as are anything that isn't a valid domain or IP.
func NewSitePolicy(mode SiteSplitMode, sites []string) *SitePolicy {
	p := &SitePolicy{mode: mode, ips: make(map[string]struct{}, len(sites))}
	seen := make(map[string]struct{}, len(sites))
	for _, s := range sites {
		s = strings.TrimSpace(strings.ToLower(strings.TrimSuffix(s, ".")))
		if s == "" {
			continue
		}
		if ip := net.ParseIP(strings.Trim(strings.TrimSpace(s), "[]")); ip != nil {
			if _, dup := p.ips[ip.String()]; !dup {
				p.ips[ip.String()] = struct{}{}
			}
			continue
		}
		s = strings.TrimPrefix(s, "*.")
		s = strings.TrimPrefix(s, ".")
		if s == "" || strings.ContainsAny(s, " /") {
			continue
		}
		if _, dup := seen[s]; dup {
			continue
		}
		seen[s] = struct{}{}
		p.sites = append(p.sites, s)
	}
	return p
}

// Enabled reports whether the policy does anything at all: an OFF mode, or a
// mode with no usable entries, is a no-op and the gateway should skip all
// sniffing/routing for it.
func (p *SitePolicy) Enabled() bool {
	return p != nil && p.mode != SiteSplitOff && (len(p.sites) > 0 || len(p.ips) > 0)
}

// matched reports whether domain (already lowercased, trailing dot trimmed)
// is exactly one of the configured sites or is a subdomain of one.
func (p *SitePolicy) matched(domain string) bool {
	for _, s := range p.sites {
		if domain == s || strings.HasSuffix(domain, "."+s) {
			return true
		}
	}
	return false
}

// matchedIP reports whether host (a literal destination address) is one of
// the configured IP entries. Weird-but-parseable forms (leading zeros, IPv4
// in IPv6 octal, ...) are passed through net.ParseIP so "0:0:0:0:0:0:0:1"
// still matches "::1".
func (p *SitePolicy) matchedIP(host string) bool {
	if host == "" {
		return false
	}
	ip := net.ParseIP(strings.Trim(host, "[]"))
	if ip == nil {
		return false
	}
	_, ok := p.ips[ip.String()]
	return ok
}

// ShouldBypass reports whether traffic for a single hostname must go around
// the tunnel. Used for DNS queries, where only a name is known - IP rules
// can't apply yet. An empty domain never matches a rule, so the policy falls
// back to its mode's default: tunnel for Exclude, direct for Include.
func (p *SitePolicy) ShouldBypass(domain string) bool {
	switch p.mode {
	case SiteSplitExclude:
		return p.matched(normalizeDomain(domain))
	case SiteSplitInclude:
		return !p.matched(normalizeDomain(domain))
	}
	return false
}

// ShouldBypassDest decides for a destination known by its literal IP and
// zero or more candidate names (the SNI sniffed off the connection, plus
// whatever the DNS cache reverse-mapped to that IP). IP rules are evaluated
// against the address, domain rules against the names:
//
//   - Exclude: bypass if the IP is listed or any candidate name is listed.
//   - Include: bypass (direct) unless the IP or any candidate name is listed.
//
// When several names share an IP and only some are listed, domainMatch
// returns true if ANY of them is, so a blocked site can never slip out of
// the tunnel (Exclude) or into it (Include) by sharing an address with an
// unlisted one - the listed entries always win the tie.
func (p *SitePolicy) ShouldBypassDest(ip string, domains []string) bool {
	ipMatch := p.matchedIP(ip)
	switch p.mode {
	case SiteSplitExclude:
		if ipMatch {
			return true
		}
		return p.domainMatch(domains)
	case SiteSplitInclude:
		if ipMatch || p.domainMatch(domains) {
			return false
		}
		return true
	}
	return false
}

// domainMatch reports whether any candidate name matches any configured
// domain site, for both the exclude and include directions.
func (p *SitePolicy) domainMatch(domains []string) bool {
	for _, d := range domains {
		if p.matched(normalizeDomain(d)) {
			return true
		}
	}
	return false
}

func normalizeDomain(domain string) string {
	return strings.TrimSuffix(strings.ToLower(strings.TrimSpace(domain)), ".")
}