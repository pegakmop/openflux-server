package gateway

import "testing"

func TestSitePolicyNormalizesEntries(t *testing.T) {
	p := NewSitePolicy(SiteSplitExclude, []string{
		"  EXAMPLE.COM ",
		"*.Sub.Example.com",
		".trailingdot.example",
		"invalid/slash",
		"",
	})
	want := []string{"example.com", "sub.example.com", "trailingdot.example"}
	if len(p.sites) != len(want) {
		t.Fatalf("got %d sites %v, want %d", len(p.sites), p.sites, len(want))
	}
	for i, w := range want {
		if p.sites[i] != w {
			t.Errorf("site[%d] = %q, want %q", i, p.sites[i], w)
		}
	}
}

func TestSitePolicyDeduplicates(t *testing.T) {
	p := NewSitePolicy(SiteSplitExclude, []string{"example.com", "*.example.com", "EXAMPLE.COM"})
	if len(p.sites) != 1 {
		t.Errorf("expected the redundant entries to collapse, got %v", p.sites)
	}
}

func TestSitePolicyOffOrEmptyIsDisabled(t *testing.T) {
	if p := NewSitePolicy(SiteSplitOff, []string{"example.com"}); p.Enabled() {
		t.Errorf("OFF policy must be disabled")
	}
	if p := NewSitePolicy(SiteSplitExclude, nil); p.Enabled() {
		t.Errorf("exclude policy with no sites must be disabled")
	}
	if p := NewSitePolicy(SiteSplitInclude, []string{"example.com"}); !p.Enabled() {
		t.Errorf("include policy with sites must be enabled")
	}
}

func TestSitePolicyExclude(t *testing.T) {
	p := NewSitePolicy(SiteSplitExclude, []string{"example.com", "blocked.org"})

	cases := []struct {
		domain string
		want   bool
	}{
		{"example.com", true},
		{"sub.example.com", true},
		{"deep.sub.example.com", true},
		{"example.com.", true}, // trailing dot is normalized away
		{"notexample.com", false},
		{"example.org", false},
		{"blocked.org", true},
		{"evil-blocked.org.com", false},
		{"", false}, // unknown -> tunnel
	}
	for _, c := range cases {
		if got := p.ShouldBypass(c.domain); got != c.want {
			t.Errorf("ShouldBypass(%q) = %v, want %v", c.domain, got, c.want)
		}
	}
}

func TestSitePolicyInclude(t *testing.T) {
	p := NewSitePolicy(SiteSplitInclude, []string{"youtube.com", "wikipedia.org"})

	cases := []struct {
		domain string
		want   bool
	}{
		{"youtube.com", false},         // tunneled
		{"www.youtube.com", false},     // subdomain tunneled
		{"wikipedia.org", false},       // tunneled
		{"en.wikipedia.org", false},    // tunneled
		{"example.com", true},          // not listed -> direct
		{"youtube.com.evil.com", true}, // suffix only
		{"", true},                     // unknown -> direct is the include default
	}
	for _, c := range cases {
		if got := p.ShouldBypass(c.domain); got != c.want {
			t.Errorf("ShouldBypass(%q) = %v, want %v", c.domain, got, c.want)
		}
	}
}

func TestSitePolicyShouldBypassDestMixesDomainsAndIPs(t *testing.T) {
	exclude := NewSitePolicy(SiteSplitExclude, []string{"blocked.org", "93.184.216.34"})
	if !exclude.ShouldBypassDest("93.184.216.34", nil) {
		t.Errorf("exclude: a listed IP must bypass")
	}
	if !exclude.ShouldBypassDest("", []string{"cdn.blocked.org"}) {
		t.Errorf("exclude: an excluded domain among the candidates must bypass")
	}
	if exclude.ShouldBypassDest("93.184.216.1", []string{"cdn.example.net"}) {
		t.Errorf("exclude: nothing listed -> keep tunneled")
	}
	if !exclude.ShouldBypassDest("93.184.216.34", []string{"anything.example"}) {
		t.Errorf("exclude: a listed IP bypasses even when the SNI name is unlisted")
	}

	include := NewSitePolicy(SiteSplitInclude, []string{"tunneled.org", "198.51.100.7"})
	if include.ShouldBypassDest("198.51.100.7", nil) {
		t.Errorf("include: a listed IP must stay tunneled")
	}
	if include.ShouldBypassDest("", []string{"cdn.tunneled.org"}) {
		t.Errorf("include: a tunneled domain among the candidates must keep the IP tunneled")
	}
	if !include.ShouldBypassDest("198.51.100.9", []string{"cdn.example.net"}) {
		t.Errorf("include: nothing listed -> direct")
	}
	if include.ShouldBypassDest("198.51.100.7", []string{"unlisted.example"}) {
		t.Errorf("include: a listed IP stays tunneled even when the SNI name is unlisted")
	}
}

func TestSitePolicySuffixWildcard(t *testing.T) {
	p := NewSitePolicy(SiteSplitExclude, []string{"*.ru", "*.co.uk"})

	cases := []struct {
		domain string
		want   bool
	}{
		{"yandex.ru", true},
		{"www.yandex.ru", true},
		{"deep.sub.example.ru", true},
		{"ru", true}, // the bare label is matched too, same as *.example.com
		{"example.com", false},
		{"ru.evil.com", false}, // suffix only, not ".ru"
		{"example.co.uk", true},
		{"shop.example.co.uk", true},
	}
	for _, c := range cases {
		if got := p.ShouldBypass(c.domain); got != c.want {
			t.Errorf("ShouldBypass(%q) = %v, want %v", c.domain, got, c.want)
		}
	}
}

func TestSitePolicyIPEntries(t *testing.T) {
	p := NewSitePolicy(SiteSplitExclude, []string{
		"93.184.216.34",
		"2606:2800:220:1:248:1893:25c8:1946",
		"[2001:4860:4860::8888]",
	})
	if !p.Enabled() {
		t.Fatalf("IP-only policy must be enabled")
	}
	if !p.ShouldBypassDest("93.184.216.34", nil) {
		t.Errorf("IPv4 entry must match")
	}
	if !p.ShouldBypassDest("2606:2800:220:1:248:1893:25c8:1946", nil) {
		t.Errorf("IPv6 entry must match")
	}
	if !p.ShouldBypassDest("2001:4860:4860::8888", nil) {
		t.Errorf("bracketed IPv6 entry must match")
	}
	// Bracket-wrapped destinations are accepted too.
	if !p.ShouldBypassDest("[93.184.216.34]", nil) {
		t.Errorf("bracket-wrapped destination must match")
	}
	if p.ShouldBypassDest("93.184.216.1", nil) {
		t.Errorf("a different IP must not match")
	}
	if p.ShouldBypassDest("", []string{"example.com"}) {
		t.Errorf("no IP and no matching domain must not bypass")
	}
}

func TestSitePolicyLightFiltering(t *testing.T) {
	p := NewSitePolicy(SiteSplitExclude, []string{"http://example.com", "exa mple.com", "example.com/path", "*.ru"})
	want := []string{"ru"}
	if len(p.sites) != 1 || p.sites[0] != want[0] {
		t.Errorf("sites = %v, want %v", p.sites, want)
	}
}
