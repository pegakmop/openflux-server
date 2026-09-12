package transport

import (
	"context"
	"fmt"
	"net"
	"syscall"
	"time"
)

// protectFD is set once per process by mobile.StartTunnel when the caller
// supplies a Protector - see ProtectedDialer's doc comment for why this
// exists. Left nil for the CLI/exit-node binary, where there is no VPN
// interface for a socket to be captured by, so Control below is then a
// no-op.
var protectFD func(fd int) bool

// SetProtector registers fn as the callback ProtectedDialer/ProtectedResolver
// route every socket they open through before it connects. Pass nil to
// disable protection again (e.g. once the tunnel stops).
func SetProtector(fn func(fd int) bool) {
	protectFD = fn
}

// ProtectedDialer returns a *net.Dialer that calls the registered protector
// (if any) on every socket it opens, before it connects.
//
// On Android, once a VpnService's tunnel is up, ALL of the device's
// outbound traffic - including the VPN app's own sockets - is routed into
// that tunnel by default. A transport's own connections (to Yandex Docs,
// its DNS lookups, ...) are exactly the traffic that's supposed to be
// carried *through* the tunnel, so without exempting them the transport
// ends up dialing itself: the connection attempt gets captured by the
// tunnel it's trying to establish, which has nowhere to forward it, and
// the whole thing deadlocks - the client can reach neither the doc nor
// DNS. VpnService.protect(fd) is Android's way to mark a socket as exempt;
// Protector (mobile.go) is how that reaches Go from Kotlin.
func ProtectedDialer() *net.Dialer {
	return &net.Dialer{
		Timeout: 30 * time.Second,
		Control: protectControl,
	}
}

// bootstrapDNSServers are queried directly (never through the tunnel, via
// ProtectedDialer) to resolve the transport's own hostnames - see
// ProtectedResolver's doc comment for why Go can't be left to pick a
// nameserver itself here. Order matters: the first one reachable wins.
// Both are well-known public resolvers chosen for being reachable from
// networks where this tool is actually used, not tied to any one profile's
// configured (tunneled) DNSUpstream.
var bootstrapDNSServers = []string{"77.88.8.8:53", "8.8.8.8:53"}

// ProtectedResolver forces the pure-Go DNS resolver and routes its lookup
// socket through the same protection. Go prefers the OS/cgo resolver on
// some platforms, which never goes through a net.Dialer at all - it would
// stay unprotected, and just as deadlocked, even with ProtectedDialer used
// everywhere else.
//
// The address Go's resolver asks Dial to connect to is not usable as-is on
// Android: Android has no /etc/resolv.conf, so Go's own dnsReadConfig can't
// read one and falls back to its hardcoded defaultNS - 127.0.0.1:53 and
// [::1]:53 (see src/net/dnsconfig_unix.go and dnsconfig.go). Nothing
// listens there, so every lookup failed with "connection refused" before
// ever reaching Yandex - not a timeout, not a capture-by-the-tunnel
// problem, just Go asking the wrong address (this is also why Go's own net
// package normally refuses to prefer the Go resolver on Android at all -
// see goosPrefersCgo in src/net/conf.go, golang/go#10714 - forcing PreferGo
// above is what makes this Dial hook run in the first place, so it has to
// make up for that). Dial below ignores whatever address it was asked for
// and queries a real public resolver directly instead.
func ProtectedResolver() *net.Resolver {
	return &net.Resolver{
		PreferGo: true,
		Dial: func(ctx context.Context, network, _ string) (net.Conn, error) {
			var lastErr error
			for _, server := range bootstrapDNSServers {
				conn, err := ProtectedDialer().DialContext(ctx, network, server)
				if err == nil {
					return conn, nil
				}
				lastErr = err
			}
			return nil, lastErr
		},
	}
}

func protectControl(network, address string, c syscall.RawConn) error {
	if protectFD == nil {
		return nil
	}
	var protectErr error
	if err := c.Control(func(fd uintptr) {
		if !protectFD(int(fd)) {
			protectErr = fmt.Errorf("failed to protect socket for %s %s", network, address)
		}
	}); err != nil {
		return err
	}
	return protectErr
}
