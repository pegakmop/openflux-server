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
	"os"
	"sync"
	"time"

	"universal-bypass-tool/gateway"
	"universal-bypass-tool/transport"
	"universal-bypass-tool/transport/yandex"
	"universal-bypass-tool/tunnel"
)

// The "oneme" (MAX) transport is intentionally not available here: it pulls
// in github.com/pion/transport/v2/stdnet -> github.com/wlynxg/anet, whose
// go:linkname hook into net's internals doesn't build against every Go
// toolchain (it does not against the one this module currently pins), and
// there is no newer anet release to fix it. The managed/key-based flow from
// controlplane is Yandex-only anyway (see nodeagent), so the mobile client
// only needs manual-yandex and key mode.

// Callback receives lifecycle and traffic updates from a running tunnel.
// Implemented on the Kotlin side; gomobile exposes it there as a Java
// interface that Go can call into.
type Callback interface {
	OnStatus(status string) // "connecting" | "connected" | "error:<message>" | "stopped"
	OnStats(bytesSent int64, bytesReceived int64)
}

// Config is the JSON contract for StartTunnel, mirroring one Android
// profile's connection fields.
type Config struct {
	Mode string `json:"mode"` // "key" (via controlplane) or "manual"

	// mode == "key"
	ControlURL string `json:"control_url"`
	KeyToken   string `json:"key_token"`

	// mode == "manual"
	Transport string `json:"transport"` // "yandex" (only option today)
	DocURL    string `json:"doc_url"`   // manual + yandex

	// Applies to both modes. MTU is informational here - the caller applies
	// it to the Android VpnService.Builder itself before opening the TUN fd.
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
// switch profiles).
func StartTunnel(tunFd int, configJSON string, cb Callback) error {
	mu.Lock()
	defer mu.Unlock()

	if current != nil {
		return fmt.Errorf("a tunnel is already running; call StopTunnel first")
	}

	var cfg Config
	if err := json.Unmarshal([]byte(configJSON), &cfg); err != nil {
		return fail(cb, fmt.Errorf("parse config: %w", err))
	}

	notify(cb, "connecting")

	docURL, err := resolveConfig(cfg)
	if err != nil {
		return fail(cb, err)
	}

	transportConfig := transport.DefaultConfig()
	trans := transport.NewCompressedTransport(yandex.NewYandexDocsTransport(docURL, transportConfig))

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

// resolveConfig returns the Yandex Docs URL to tunnel through. Only that one
// transport is supported here (see the package-level comment on why "oneme"
// is excluded), so there is nothing else for a caller to switch on.
func resolveConfig(cfg Config) (docURL string, err error) {
	switch cfg.Mode {
	case "key":
		if cfg.ControlURL == "" || cfg.KeyToken == "" {
			return "", fmt.Errorf(`mode "key" requires control_url and key_token`)
		}
		_, parsed, rerr := resolveRaw(cfg.ControlURL, cfg.KeyToken)
		if rerr != nil {
			return "", rerr
		}
		if parsed.Status != "active" {
			return "", fmt.Errorf("key is %s", parsed.Status)
		}
		if parsed.Transport != "" && parsed.Transport != "yandex" {
			return "", fmt.Errorf("unsupported transport %q for this client", parsed.Transport)
		}
		return parsed.DocURL, nil

	case "manual":
		t := cfg.Transport
		if t == "" {
			t = "yandex"
		}
		if t != "yandex" {
			return "", fmt.Errorf("unsupported transport %q for this client", t)
		}
		if cfg.DocURL == "" {
			return "", fmt.Errorf("manual mode requires doc_url")
		}
		return cfg.DocURL, nil

	default:
		return "", fmt.Errorf(`config.mode must be "key" or "manual", got %q`, cfg.Mode)
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
