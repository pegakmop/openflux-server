// Package mobile is the sole entry point bound into an Android .aar via
// `gomobile bind` (see build_android_aar.sh). gomobile's bridging only
// supports exported functions/structs/interfaces built from a small set of
// types (string, []byte, bool, numeric types, error, and other bound
// interfaces/structs - no generics, no raw channels or maps), so this
// package stays a thin façade over transport/tunnel/gateway rather than
// exposing those packages' own richer APIs directly.
package mobile

import (
	"encoding/json"
	"fmt"
	"net"
	"os"
	"sync"
	"time"

	"universal-bypass-tool/gateway"
	"universal-bypass-tool/transport"
	"universal-bypass-tool/transport/oneme"
	"universal-bypass-tool/transport/yandex"
	"universal-bypass-tool/tunnel"
	"universal-bypass-tool/utils"
)

// originalResolver is whatever net.DefaultResolver was before StartTunnel
// first overrides it (see Protector below) - captured once at package load,
// before anything has a chance to change it, so StopTunnel can put it back.
var originalResolver = net.DefaultResolver

// The "oneme" (MAX) transport pulls in github.com/pion/transport/v2/stdnet
// -> github.com/wlynxg/anet, which uses a //go:linkname hook into net's
// internals that Go 1.23+'s linker rejects by default ("invalid reference
// to net.zoneCache"). It's not actually unfixable - anet's own README
// documents the fix - it just needs `-ldflags=-checklinkname=0` passed to
// `gomobile bind` (see build_android_aar.sh), which isn't something the Go
// toolchain can be told to do from within this source file.

// Callback receives lifecycle and traffic updates from a running tunnel.
// Implemented on the Kotlin side; gomobile exposes it there as a Java
// interface that Go can call into.
type Callback interface {
	OnStatus(status string) // "connecting" | "connected" | "error:<message>" | "stopped"
	OnStats(bytesSent int64, bytesReceived int64)
	// OnLogEvent reports a fine-grained connection-lifecycle event for a
	// human-facing log feed, distinct from OnStatus's coarse current-state
	// snapshot: OnStatus only ever says "connected" once per StartTunnel
	// call, while the transport keeps silently reconnecting in the
	// background after that - these events are what makes that visible.
	// code/detail are transport.Event* constants and their documented
	// detail shapes (see transport/transport.go).
	OnLogEvent(code string, detail string)
	// OnRawLog delivers one line of the engine's internal debug log
	// (raw sockets, transport internals, ...) - only fires when
	// Config.VerboseLogging is set.
	OnRawLog(line string)
}

// Protector exempts a raw socket fd from the Android VPN's own tunnel
// interface (Kotlin implements this as a thin call to
// android.net.VpnService.protect(fd)). Without it, every connection the
// transport itself makes - to the doc, to DNS, ... - gets captured by the
// very tunnel it's supposed to be carrying, which deadlocks the whole
// thing (see transport.ProtectedDialer's doc comment for the full story).
// May be nil - StartTunnel then runs unprotected, which is correct for
// callers that aren't behind an Android VpnService (there's nothing to be
// captured by).
type Protector interface {
	Protect(fd int) bool
}

// Config is the JSON contract for StartTunnel, mirroring one Android
// profile's connection fields. There used to be a second "key" mode where
// StartTunnel itself resolved a controlplane key token into a doc_url via a
// live HTTPS request (mobile/resolve.go's ResolveKey, since removed
// entirely) - that request had none of the tunnel's own disguise and was
// trivial for a hostile network to block outright. The Android app's
// "Fork" profile mode (formerly "key" mode) now gets doc_url exclusively
// from data already embedded in an imported deep link or pasted in by
// hand (see ProfileDeepLink.kt) - there is no code path left, one-off or
// otherwise, that has this app make a live request to a controlplane to
// learn where to connect. StartTunnel always receives the plain doc_url
// below regardless of which UI mode produced it.
type Config struct {
	Mode string `json:"mode"` // must be "manual" - see buildTransport

	Transport string `json:"transport"` // "yandex" (default), "volga", "max", or "yandex_multistream"
	DocURL    string `json:"doc_url"`   // yandex, volga
	MaxToken  string `json:"max_token"` // max: your MAX account's own auth token
	MaxUID    int64  `json:"max_uid"`   // max: the contact's user ID to place the call to

	// DocURLs is yandex_multistream's doc_url: 2+ independent Yandex Docs
	// sessions. The exit node needs the exact same list, any order.
	DocURLs []string `json:"doc_urls,omitempty"`

	KeyToken string `json:"key_token,omitempty"`

	// E2EEncryption + a non-blank KeyToken wraps the transport in
	// transport.NewEncryptedTransport. Kept separate from "KeyToken is
	// non-blank" - KeyToken is already carried by every KEY-mode profile
	// for unrelated reasons, and the client has no fallback if it encrypts
	// against an exit node that can't (only the exit node auto-detects).
	E2EEncryption bool `json:"e2e_encryption,omitempty"`

	// MTU is informational here - the caller applies it to the Android
	// VpnService.Builder itself before opening the TUN fd.
	MTU         int    `json:"mtu"`
	DNSUpstream string `json:"dns_upstream"`

	// SiteSplitMode is the per-site split-tunneling mode: "" or "off"
	// (everything through the tunnel), "exclude" (everything except the
	// sites below), or "include" (only the sites below). SiteSplitSites is
	// the list of domains - accepting suffix wildcards like "*.ru" and
	// literal IPs too - those rules apply to. See gateway.SitePolicy -
	// listeners don't set it; only the client's StartTunnel consumes it.
	SiteSplitMode  string   `json:"site_split_mode,omitempty"`
	SiteSplitSites []string `json:"site_split_sites,omitempty"`

	// ForceBootstrapDNS, when set, replaces the fixed public resolvers
	// StartTunnel would otherwise use to resolve the transport's own
	// hostnames (docs.yandex.ru and friends) before the tunnel exists to
	// carry anything else - see transport.SetBootstrapDNSServers. Empty
	// leaves the defaults in place. This is about getting the very first
	// connection off the ground on a network whose own resolver can't reach
	// (or refuses to answer for) those two public ones; it has no bearing on
	// DNSUpstream above, which is queried only once the tunnel is already up.
	ForceBootstrapDNS string `json:"force_bootstrap_dns,omitempty"`

	// VerboseLogging turns on the engine's internal debug log (raw sockets,
	// transport internals, ...) for this session, delivered via
	// Callback.OnRawLog.
	VerboseLogging bool `json:"verbose_logging,omitempty"`
}

type session struct {
	trans     transport.Transport
	tun       *tunnel.TCPTunnel
	gw        *gateway.Server
	tunFile   *os.File
	stopStats chan struct{}
}

var (
	mu      sync.Mutex
	current *session
)

// StartTunnel brings up a full client tunnel bound to tunFd - a TUN file
// descriptor already established by the caller's VpnService - and relays
// its traffic through the transport described by configJSON. Only one
// tunnel runs at a time; call StopTunnel before starting another (e.g. to
// switch profiles). protector may be nil (see Protector's doc comment).
func StartTunnel(tunFd int, configJSON string, protector Protector, cb Callback) error {
	mu.Lock()
	defer mu.Unlock()

	if current != nil {
		return fmt.Errorf("a tunnel is already running; call StopTunnel first")
	}

	var cfg Config
	if err := json.Unmarshal([]byte(configJSON), &cfg); err != nil {
		return fail(cb, fmt.Errorf("parse config: %w", err))
	}

	utils.SetVerbose(cfg.VerboseLogging)
	if cfg.VerboseLogging {
		utils.SetLogSink(func(line string) {
			if cb != nil {
				cb.OnRawLog(line)
			}
		})
	} else {
		utils.SetLogSink(nil)
	}

	if protector != nil {
		transport.SetProtector(protector.Protect)
		net.DefaultResolver = transport.ProtectedResolver()
		if cfg.ForceBootstrapDNS != "" {
			transport.SetBootstrapDNSServers([]string{cfg.ForceBootstrapDNS})
		}
	}

	notify(cb, "connecting")

	transportConfig := transport.DefaultConfig()
	trans, wrapped, err := buildTransport(cfg, transportConfig)
	if err != nil {
		return fail(cb, err)
	}

	if !wrapped {
		if cfg.E2EEncryption && cfg.KeyToken != "" {
			trans = transport.NewEncryptedTransport(trans, cfg.KeyToken, false)
		}
		trans = transport.NewCompressedTransport(trans)
	}
	trans.SetEventCallback(func(code, detail string) {
		if cb != nil {
			cb.OnLogEvent(code, detail)
		}
	})

	if err := trans.Start(); err != nil {
		return fail(cb, fmt.Errorf("start transport: %w", err))
	}

	tun := tunnel.NewTCPTunnel(trans, false)

	tunFile := os.NewFile(uintptr(tunFd), "tun")
	if tunFile == nil {
		trans.Stop()
		tun.Close()
		return fail(cb, fmt.Errorf("invalid tun file descriptor: %d", tunFd))
	}

	gw := gateway.NewServerWithPolicy(tun, cfg.DNSUpstream, sitePolicy(cfg))
	if err := gw.Start(tunFile, tunFile); err != nil {
		trans.Stop()
		tun.Close()
		tunFile.Close()
		return fail(cb, fmt.Errorf("start gateway: %w", err))
	}

	s := &session{trans: trans, tun: tun, gw: gw, tunFile: tunFile, stopStats: make(chan struct{})}
	current = s

	go pumpStats(s, cb)
	notify(cb, "connected")
	return nil
}

// StopTunnel tears down the currently running tunnel, if any. Safe to call
// when nothing is running.
func StopTunnel() error {
	mu.Lock()
	s := current
	current = nil
	mu.Unlock()

	transport.SetProtector(nil)
	transport.SetBootstrapDNSServers(nil)
	net.DefaultResolver = originalResolver
	utils.SetLogSink(nil)

	if s == nil {
		return nil
	}

	close(s.stopStats)
	s.gw.Close()      // marks it closed and destroys its gvisor stack
	s.tunFile.Close() // unblocks the gateway's tun read loop
	s.trans.Stop()
	s.tun.Close()
	return nil
}

// buildTransport picks and constructs the transport for cfg. wrapped
// reports whether it already carries its own compression/encryption
// (yandex_multistream does this per-stream) - StartTunnel skips its own
// wrapping when true.
func buildTransport(cfg Config, transportConfig transport.TransportConfig) (trans transport.Transport, wrapped bool, err error) {
	if cfg.Mode != "manual" {
		return nil, false, fmt.Errorf(`config.mode must be "manual", got %q`, cfg.Mode)
	}

	t := cfg.Transport
	if t == "" {
		t = "yandex"
	}
	switch t {
	case "yandex":
		if cfg.DocURL == "" {
			return nil, false, fmt.Errorf("doc_url is required")
		}
		return yandex.NewYandexDocsTransport(cfg.DocURL, transportConfig), false, nil
	case "volga":
		if cfg.DocURL == "" {
			return nil, false, fmt.Errorf("doc_url is required")
		}
		return yandex.NewYandexVolgaTransport(cfg.DocURL, transportConfig), false, nil
	case "max":
		if cfg.MaxToken == "" || cfg.MaxUID == 0 {
			return nil, false, fmt.Errorf("the max transport requires max_token and max_uid")
		}
		return oneme.NewOneMeTransport(false, cfg.MaxToken, cfg.MaxUID, transportConfig), false, nil
	case "yandex_multistream":
		if len(cfg.DocURLs) < 2 {
			return nil, false, fmt.Errorf("yandex_multistream requires at least 2 doc_urls")
		}
		streams := make([]transport.Transport, len(cfg.DocURLs))
		for i, url := range cfg.DocURLs {
			var st transport.Transport = yandex.NewYandexDocsTransport(url, transportConfig)
			if cfg.E2EEncryption && cfg.KeyToken != "" {
				st = transport.NewEncryptedTransportForStream(st, cfg.KeyToken, false, i)
			}
			streams[i] = transport.NewCompressedTransport(st)
		}
		return transport.NewMultiStreamTransport(streams), true, nil
	default:
		return nil, false, fmt.Errorf("unsupported transport %q for this client", t)
	}
}

// sitePolicy turns the client config's site-split fields into the gateway's
// route policy. An empty/unset mode or empty site list yields a disabled
// policy (identical to today's always-tunnel behavior).
func sitePolicy(cfg Config) *gateway.SitePolicy {
	return gateway.NewSitePolicy(gateway.ParseSiteSplitMode(cfg.SiteSplitMode), cfg.SiteSplitSites)
}

func pumpStats(s *session, cb Callback) {
	ticker := time.NewTicker(2 * time.Second)
	defer ticker.Stop()
	for {
		select {
		case <-s.stopStats:
			return
		case <-ticker.C:
			stats := s.trans.Stats()
			if cb != nil {
				cb.OnStats(int64(stats.BytesSent), int64(stats.BytesReceived))
			}
		}
	}
}

func notify(cb Callback, status string) {
	if cb != nil {
		cb.OnStatus(status)
	}
}

func fail(cb Callback, err error) error {
	notify(cb, "error:"+err.Error())
	return err
}
