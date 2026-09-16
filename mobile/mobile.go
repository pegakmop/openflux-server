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
	"universal-bypass-tool/socks5"
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

type socks5Session struct {
	trans  transport.Transport
	tun    *tunnel.TCPTunnel
	server *socks5.SOCKS5Server
}

var (
	socksMu      sync.Mutex
	currentSocks *socks5Session
)

// StartTunnel brings up a full client tunnel bound to tunFd - a TUN file
// descriptor already established by the caller's VpnService - and relays
// its traffic through the transport described by configJSON. Only one
// tunnel runs at a time; call StopTunnel before starting another (e.g. to
// switch profiles). protector may be nil (see Protector's doc comment).
func StartTunnel(tunFd int, configJSON string, protector Protector, cb Callback) error {
	mu.Lock()
	defer mu.Unlock()
	socksMu.Lock()
	running := currentSocks != nil
	socksMu.Unlock()
	if current != nil {
		return fmt.Errorf("a tunnel is already running; call StopTunnel first")
	}
	if running {
		return fmt.Errorf("a SOCKS5 proxy is already running; call StopSocks5Proxy first")
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
	trans, err := buildTransport(cfg, transportConfig)
	if err != nil {
		return fail(cb, err)
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

// NetworkChanged tells the running tunnel's transport to retry right now
// instead of waiting to notice on its own - see
// transport.Transport.ForceReconnect's doc comment for why that matters on
// a network that goes silent instead of resetting the connection. The
// caller (Android's ConnectivityManager, or the equivalent on another
// platform) already knows the active network changed well before a read or
// write on the old one would ever time out. A safe no-op when nothing is
// running.
func NetworkChanged() {
	mu.Lock()
	s := current
	mu.Unlock()
	if s != nil {
		s.trans.ForceReconnect()
	}
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

// StartSocks5Proxy brings up a local SOCKS5 (CONNECT-only, TCP, no auth)
// proxy on listenAddr (e.g. "127.0.0.1:1080") and relays every connection
// made to it through the transport described by configJSON - the same
// transport/tunnel stack StartTunnel uses, minus the TUN/VpnService/gateway
// layer. Unlike StartTunnel this doesn't capture the device's traffic at
// all: only whatever the caller explicitly points at listenAddr goes through
// it, so no VPN permission and no Protector is needed here - there's no
// self-created tunnel interface for this process's own connections to loop
// back into. Only one of StartTunnel/StartSocks5Proxy runs at a time; call
// the matching Stop function first to switch between them.
//
// A SOCKS5 CONNECT target's hostname (not just a literal IP) is resolved by
// the device's normal DNS resolver before the connection ever reaches the
// tunnel - same as the desktop client's --socks5 mode (see
// socks5.SOCKS5Server/tunnel.TCPTunnel.DialTCP) - only the resulting IP
// traffic is tunneled, not the DNS lookup itself.
func StartSocks5Proxy(configJSON string, listenAddr string, cb Callback) error {
	socksMu.Lock()
	defer socksMu.Unlock()
	mu.Lock()
	running := current != nil
	mu.Unlock()
	if currentSocks != nil {
		return fmt.Errorf("a SOCKS5 proxy is already running; call StopSocks5Proxy first")
	}
	if running {
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

	notify(cb, "connecting")

	transportConfig := transport.DefaultConfig()
	trans, err := buildTransport(cfg, transportConfig)
	if err != nil {
		return fail(cb, err)
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
	server := socks5.NewSOCKS5Server(listenAddr, tun)

	s := &socks5Session{trans: trans, tun: tun, server: server}
	currentSocks = s

	go func() {
		if err := server.Start(); err != nil {
			utils.Debugf("[SOCKS5] server stopped: %v", err)
		}
	}()

	notify(cb, "connected")
	return nil
}

// StopSocks5Proxy tears down the currently running SOCKS5 proxy, if any.
// Safe to call when nothing is running.
func StopSocks5Proxy() error {
	socksMu.Lock()
	s := currentSocks
	currentSocks = nil
	socksMu.Unlock()

	utils.SetLogSink(nil)

	if s == nil {
		return nil
	}

	s.server.Stop()
	s.trans.Stop()
	s.tun.Close()
	return nil
}

// buildTransport picks, constructs, and fully wires (compression/encryption
// included - see wrapYandex/wrapGeneric) the transport for cfg. The result
// is ready for Start; callers don't need to wrap it any further.
func buildTransport(cfg Config, transportConfig transport.TransportConfig) (transport.Transport, error) {
	if cfg.Mode != "manual" {
		return nil, fmt.Errorf(`config.mode must be "manual", got %q`, cfg.Mode)
	}

	t := cfg.Transport
	if t == "" {
		t = "yandex"
	}
	switch t {
	case "yandex":
		if cfg.DocURL == "" {
			return nil, fmt.Errorf("doc_url is required")
		}
		return wrapYandex(yandex.NewYandexDocsTransport(cfg.DocURL, transportConfig), cfg, -1), nil
	case "volga":
		if cfg.DocURL == "" {
			return nil, fmt.Errorf("doc_url is required")
		}
		return wrapGeneric(yandex.NewYandexVolgaTransport(cfg.DocURL, transportConfig), cfg), nil
	case "max":
		if cfg.MaxToken == "" || cfg.MaxUID == 0 {
			return nil, fmt.Errorf("the max transport requires max_token and max_uid")
		}
		return wrapGeneric(oneme.NewOneMeTransport(false, cfg.MaxToken, cfg.MaxUID, transportConfig), cfg), nil
	case "yandex_multistream":
		if len(cfg.DocURLs) < 2 {
			return nil, fmt.Errorf("yandex_multistream requires at least 2 doc_urls")
		}
		streams := make([]transport.Transport, len(cfg.DocURLs))
		for i, url := range cfg.DocURLs {
			streams[i] = wrapYandex(yandex.NewYandexDocsTransport(url, transportConfig), cfg, i)
		}
		return transport.NewMultiStreamTransport(streams), nil
	default:
		return nil, fmt.Errorf("unsupported transport %q for this client", t)
	}
}

// wrapYandex applies this profile's E2E setting to a Yandex transport.
// streamIdx selects the per-stream key derivation for one leg of a
// yandex_multistream profile (matching nodeagent's own per-stream keying,
// so both ends derive the same per-stream keys); -1 means the
// single-stream case. Either way, yd manages its own compression (and, with
// encryption on, its own encryption) internally rather than being wrapped in
// anything - see YandexDocsTransport.EnableSelfCompression and
// EnableEncryptedSelfCompression's doc comments for why that's strictly
// better than the traditional transport.CompressedTransport(transport.EncryptedTransport(...))
// wrapping (never worse, and better once the peer's keepalive proves it
// also upgraded) while remaining fully compatible with a peer still using
// that traditional wrapping.
func wrapYandex(yd *yandex.YandexDocsTransport, cfg Config, streamIdx int) transport.Transport {
	if !cfg.E2EEncryption || cfg.KeyToken == "" {
		yd.EnableSelfCompression()
		return yd
	}
	if streamIdx >= 0 {
		yd.EnableEncryptedSelfCompressionForStream(cfg.KeyToken, false, streamIdx)
	} else {
		yd.EnableEncryptedSelfCompression(cfg.KeyToken, false)
	}
	return yd
}

// wrapGeneric applies E2E encryption (if configured), then compression, to
// any transport that doesn't manage its own the way Yandex now can - volga
// and max, unchanged from before this feature existed.
func wrapGeneric(inner transport.Transport, cfg Config) transport.Transport {
	if cfg.E2EEncryption && cfg.KeyToken != "" {
		inner = transport.NewEncryptedTransport(inner, cfg.KeyToken, false)
	}
	return transport.NewCompressedTransport(inner)
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
