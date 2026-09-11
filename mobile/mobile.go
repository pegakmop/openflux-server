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
// live HTTPS request (see resolve.go's ResolveKey) - that request had none
// of the tunnel's own disguise and was trivial for a hostile network to
// block outright, so the Android app now resolves once (an explicit,
// user-initiated action - see ProfileEditScreen's "check key") and caches
// the result, always calling StartTunnel with the plain doc_url below.
// ResolveKey itself is unchanged and still used for that one-off check.
type Config struct {
	Mode string `json:"mode"` // must be "manual" - see buildTransport

	Transport string `json:"transport"` // "yandex" (default), "volga", or "max"
	DocURL    string `json:"doc_url"`   // yandex, volga
	MaxToken  string `json:"max_token"` // max: your MAX account's own auth token
	MaxUID    int64  `json:"max_uid"`   // max: the contact's user ID to place the call to

	// MTU is informational here - the caller applies it to the Android
	// VpnService.Builder itself before opening the TUN fd.
	MTU         int    `json:"mtu"`
	DNSUpstream string `json:"dns_upstream"`
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

	if protector != nil {
		transport.SetProtector(protector.Protect)
		net.DefaultResolver = transport.ProtectedResolver()
	}

	notify(cb, "connecting")

	transportConfig := transport.DefaultConfig()
	inner, err := buildTransport(cfg, transportConfig)
	if err != nil {
		return fail(cb, err)
	}

	trans := transport.NewCompressedTransport(inner)
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

	gw := gateway.NewServer(tun, cfg.DNSUpstream)
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
	net.DefaultResolver = originalResolver

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

// buildTransport picks and constructs the concrete transport for cfg,
// without starting it - StartTunnel wraps the result (compression, event
// callback) before calling Start.
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
		return yandex.NewYandexDocsTransport(cfg.DocURL, transportConfig), nil
	case "volga":
		if cfg.DocURL == "" {
			return nil, fmt.Errorf("doc_url is required")
		}
		return yandex.NewYandexVolgaTransport(cfg.DocURL, transportConfig), nil
	case "max":
		if cfg.MaxToken == "" || cfg.MaxUID == 0 {
			return nil, fmt.Errorf("the max transport requires max_token and max_uid")
		}
		return oneme.NewOneMeTransport(false, cfg.MaxToken, cfg.MaxUID, transportConfig), nil
	default:
		return nil, fmt.Errorf("unsupported transport %q for this client", t)
	}
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
				gwIn, gwOut := s.gw.PacketCounts()
				tunIn, tunOut := s.tun.PacketCounts()
				cb.OnLogEvent("debug", fmt.Sprintf(
					"gateway(tun->app) in=%d out=%d | tunnel(app->transport) in=%d out=%d | transport sent=%d recv=%d",
					gwIn, gwOut, tunIn, tunOut, stats.BytesSent, stats.BytesReceived,
				))
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
