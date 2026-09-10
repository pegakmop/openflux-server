# OpenFlux Server

**English** | [Русский](README.ru.md)

A fork of [p1neappleXpress/OpenFlux](https://github.com/p1neappleXpress/OpenFlux). Network stack
research tool: TCP tunnel with pluggable transports, plus a multi-user control plane and gomobile
bindings for the [Android app](https://github.com/wlruscfd/openflux-app).

Sibling repos: [openflux-app](https://github.com/wlruscfd/openflux-app) (the Android client) and
[openflux-deploy](https://github.com/wlruscfd/openflux-deploy) (rolls this repo's `controlplane`
out onto a VPS).

## Overview
```
Client (SOCKS5) --> Transport --> Exit Node --> Internet
```

TCP packets are sent via Transport. Currently, there are two transports available:
1. Yandex - sends packets via Yandex Docs cursor messages;
2. Max - sends packets via WebRTC DataChannel (desktop client/exit-node only - the Android app
   doesn't support it; see `mobile/mobile.go`'s package comment for why).

Client side runs a SOCKS5 proxy, exit node decapsulates and forwards packets to destination point.

## Requirements
1. Golang v. 1.26.3+ - for building the desktop client / exit-node binary (universal-bypass-tool);
2. Android NDK v.27.0.12077973+ - for building the `.aar` the Android app embeds (`./build_android_aar.sh`);
3. XCode v. 26.6+ - for building the iOS client binary;
4. A Linux VPS/VDS for the exit node and, if you want the multi-user control plane, for `controlplane` too (see [openflux-deploy](https://github.com/wlruscfd/openflux-deploy)).

## Structure

```
main.go
transport/
├── transport.go      # Transport interface
├── yandex/           # Yandex Docs backend
└── oneme/            # MAX Messenger backend (desktop only)
tunnel/
├── tunnel.go         # TCP tunnel core
├── endpoint.go       # Virtual NIC
└── rawsocket.go      # Raw socket (exit node)
socks5/                # SOCKS5 server (desktop client)
gateway/               # TUN-based transparent proxy (Android client, via VpnService)
mobile/                # gomobile bind entry point consumed by openflux-app
nodeagent/             # Exit-node orchestrator for managed (controlplane) mode
controlplane/          # Multi-user key/traffic/token service + admin panel - separate Go
│                      # module, see controlplane/README.md
network/               # Checksums, packet parsing
utils/                 # Debug logging
```

## Build (desktop client / exit-node binary)

```bash
go mod tidy
go build -o universal-bypass-tool .
```

## Build for Android
See [openflux-app](https://github.com/wlruscfd/openflux-app)'s README - `./build_android_aar.sh`
here builds `mobile/` into an `.aar` via `gomobile bind` for that repo to embed.

## Build for iOS (client binary)
```bash
export XCODE_PATH="<your Xcode.app path>" # optional, defaults to /Applications/Xcode.app
./build_ios.sh
```

## Usage

### Setting up an exit node
1. You must have root access on the exit-node machine;
2. Only the legacy Yandex document editor is supported (toggle this from the interface).

```bash
sudo iptables -A OUTPUT -p tcp --tcp-flags RST RST -j DROP
sudo ./universal-bypass-tool --exit-node --url "YOUR_YANDEX_DOC_URL" --debug
```

### Setting up a desktop client

```bash
./universal-bypass-tool --client --url "YOUR_YANDEX_DOC_URL" --socks5 :1080 --debug
```

Then set up a SOCKS5 proxy in your browser at localhost:1080.

## Flags

| Flag          | Default             | Description                |
|---------------|---------------------|----------------------------|
| `--client`    |                     | Run as client              |
| `--exit-node` |                     | Run as exit node           |
| `--socks5`    | `:1080`             | SOCKS5 listen address      |
| `--url`       | `https://localhost` | Document URL (Yandex Docs) |
| `--maxToken`  | ``                  | Auth token (Max)           |
| `--maxUid`    | ``                  | User ID (Max)              |
| `--debug`     | `false`             | Enable verbose logging     |
| `--transport` | `yandex`            | Select transport backend   |
| `--managed`      | `false` | Exit node only: fetch active keys from a controlplane instance instead of a single `--url` |
| `--control-url`  | ``      | Managed mode: base URL of the `openflux-control` service |
| `--node-token`   | ``      | Managed mode: this node's bearer token from controlplane |

## Multi-user deployments (controlplane)

For running many keys/users behind a fleet of exit nodes — auth tokens, per-key traffic
accounting, enabling/disabling keys, a web admin panel, and an ingestion API for third-party key
generators — see [controlplane/README.md](controlplane/README.md). Exit nodes opt into this with
`--exit-node --managed --control-url ... --node-token ...`; the plain single-`--url` flow above
still works unchanged for manual/one-off use. To actually roll `controlplane` out onto a VPS
(Postgres, systemd, Nginx, Let's Encrypt), see
[openflux-deploy](https://github.com/wlruscfd/openflux-deploy).

## Implementing custom transports

You are free to implement the `Transport` interface from `transport/transport.go` and register
your custom transport in `main.go`'s switch block.

## License

This project is licensed under the **GNU General Public License v3.0 or later**.
See [LICENSE](LICENSE) for the full text.

Third-party licenses are listed in [NOTICE](NOTICE).

## Disclaimer

Educational use only. Test on your own machines and networks.
