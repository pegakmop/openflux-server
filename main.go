package main

import (
	"context"
	"flag"
	"fmt"
	_ "github.com/wlynxg/anet"
	"log"
	"os"
	"os/signal"
	rtdebug "runtime/debug"
	"strconv"
	"strings"
	"syscall"
	"universal-bypass-tool/nodeagent"
	"universal-bypass-tool/socks5"
	"universal-bypass-tool/transport"
	"universal-bypass-tool/transport/cupsonline"
	"universal-bypass-tool/transport/mailru"
	"universal-bypass-tool/transport/oneme"
	"universal-bypass-tool/transport/yandex"
	"universal-bypass-tool/tunnel"
	"universal-bypass-tool/utils"
)

var (
	globalDocUrl string
	maxToken     string
	maxUid       string
)

func main() {
	fmt.Print("written by p1neappleXpress\n")

	exitNode := flag.Bool("exit-node", false, "Run as exit node (needs root)")
	client := flag.Bool("client", false, "Run as client")
	debug := flag.Bool("debug", false, "Enable verbose debug logging")
	socksAddr := flag.String("socks5", ":1080", "SOCKS5 address")
	transportType := flag.String("transport", "yandex", "Transport type (yandex, volga, oneme, yandex_multistream, cupsonline, mailru)")
	managed := flag.Bool("managed", false, "Exit node only: fetch active keys from a controlplane instance instead of a single --url")
	controlURL := flag.String("control-url", "", "Managed mode: base URL of the openflux-control service")
	nodeToken := flag.String("node-token", "", "Managed mode: this node's bearer token from controlplane")
	mode := flag.String("mode", "raw", "Exit node only: 'raw' (default, needs root; raw socket + gvisor NAT, forwards any IP protocol) or 'proxy' (no root, no raw socket; TCP only - see README)")
	localIP := flag.String("local-ip", "", "Raw mode only: exit node egress IP, so the RST-drop iptables rule can be scoped with -s instead of host-wide")
	codec := flag.String("codec", "legacy", "Wire codec for --transport volga/oneme/cupsonline/mailru: 'legacy' (default, per-packet LZ4 - unchanged) or 'batched' (coalesce bursts into one zstd-compressed message per transport send; both ends must agree - see README). Ignored for yandex/yandex_multistream, which auto-negotiate their own whole-batch zstd format with the peer - see README.")
	flag.StringVar(&globalDocUrl, "url", "http://#", "Document URL. If u use Yandex.Docs transport")
	docUrls := flag.String("urls", "", "Comma-separated doc URLs for --transport yandex_multistream (2+ required, same list on both ends)")
	flag.StringVar(&maxToken, "maxToken", "", "MAX call user id. If u use MAX transport")
	flag.StringVar(&maxUid, "maxUid", "", "MAX Web token. If u use MAX transport")
	flag.Parse()

	if !*exitNode && !*client {
		flag.Usage()
		os.Exit(1)
	}

	if *debug {
		utils.EnableDebug()
	}

	exitMode, modeErr := tunnel.ParseExitMode(*mode)
	if modeErr != nil {
		log.Fatalf("%v", modeErr)
	}
	if *localIP != "" {
		tunnel.SetLocalIP(*localIP)
	}
	if *exitNode {
		// The exit node often runs on a small VPS; keep the heap tight
		// under load instead of crashing (set GOMEMLIMIT in the
		// environment for a hard cap on top of this). Per-packet debug
		// logging is the main allocation source under real traffic - avoid
		// --debug in production regardless of this.
		rtdebug.SetGCPercent(20)
	}

	if *managed {
		if !*exitNode {
			log.Fatalf("--managed is only valid together with --exit-node")
		}
		if *controlURL == "" || *nodeToken == "" {
			log.Fatalf("--managed requires --control-url and --node-token")
		}

		log.Printf("=== Universal Bypass Tool ===")
		log.Printf("Mode: EXIT NODE (managed, control=%s, exit-mode=%s)", *controlURL, exitMode)

		ctx, stop := signal.NotifyContext(context.Background(), syscall.SIGINT, syscall.SIGTERM)
		defer stop()

		cfg := nodeagent.DefaultConfig(*controlURL, *nodeToken)
		cfg.ExitMode = exitMode
		orch := nodeagent.NewOrchestrator(cfg)
		orch.Run(ctx)
		return
	}

	log.Printf("=== Universal Bypass Tool ===")
	log.Printf("Mode: %s", map[bool]string{true: "EXIT NODE", false: "CLIENT"}[*exitNode])
	log.Printf("Transport: %s", *transportType)

	config := transport.DefaultConfig()
	var trans transport.Transport

	wrapCodec := func(inner transport.Transport) transport.Transport {
		wrapped, err := transport.WrapCodec(inner, *codec)
		if err != nil {
			log.Fatalf("%v", err)
		}
		return wrapped
	}

	// selfCompressingYandex builds a Yandex transport that manages its own
	// per-batch compression (see YandexDocsTransport.EnableSelfCompression)
	// instead of --codec's generic wrapping - it auto-negotiates with the
	// peer and is never worse than --codec=legacy, so --codec is ignored
	// for this transport type (it still applies to volga/oneme below).
	selfCompressingYandex := func(url string) transport.Transport {
		yd := yandex.NewYandexDocsTransport(url, config)
		yd.EnableSelfCompression()
		return yd
	}

	switch *transportType {
	case "yandex":
		trans = selfCompressingYandex(globalDocUrl)
	case "volga":
		trans = wrapCodec(yandex.NewYandexVolgaTransport(globalDocUrl, config))
	case "oneme":
		uidint, _ := strconv.ParseInt(maxUid, 10, 64)
		trans = wrapCodec(oneme.NewOneMeTransport(*exitNode, maxToken, uidint, config))
	case "cupsonline":
		trans = wrapCodec(cupsonline.NewCupsonlineTransport(globalDocUrl, config, !*exitNode))
	case "mailru":
		trans = wrapCodec(mailru.NewMailruDocsTransport(globalDocUrl, config))
	case "yandex_multistream":
		urls := strings.Split(*docUrls, ",")
		if len(urls) < 2 {
			log.Fatalf("--transport yandex_multistream requires --urls with 2+ comma-separated doc URLs")
		}
		streams := make([]transport.Transport, len(urls))
		for i, url := range urls {
			streams[i] = selfCompressingYandex(strings.TrimSpace(url))
		}
		trans = transport.NewMultiStreamTransport(streams)
	default:
		log.Fatalf("Unknown transport type: %s", *transportType)
	}

	if err := trans.Start(); err != nil {
		log.Fatalf("Failed to start transport: %v", err)
	}

	tun := tunnel.NewTCPTunnelMode(trans, *exitNode, exitMode)
	// Read back the tunnel's actual mode, not the requested one: raw mode
	// silently falls back to proxy mode if raw-socket creation fails (e.g.
	// missing root), and printing the requested mode here would tell the
	// operator to apply iptables rules a proxy-mode node doesn't need.
	exitMode = tun.ExitMode()

	if *exitNode {
		if exitMode == tunnel.ExitModeProxy {
			log.Printf("Running as EXIT NODE (proxy mode - no root, no raw socket, TCP only)")
		} else {
			log.Printf("Running as EXIT NODE (raw mode, needs root for raw socket)")
			if *localIP != "" {
				// Scoped: only drop kernel RSTs from the tunnel's own
				// egress IP, leaving the host's other services untouched.
				log.Printf("! Run: sudo iptables -A OUTPUT -p tcp --tcp-flags RST RST -s %s -j DROP", *localIP)
			} else {
				log.Printf("! Kernel RSTs would tear down tunnel connections. Prefer a scoped rule:")
				log.Printf("!   assign a dedicated alias IP, run with --local-ip <ip>, then:")
				log.Printf("!   sudo iptables -A OUTPUT -p tcp --tcp-flags RST RST -s <ip> -j DROP")
				log.Printf("! Host-wide fallback (drops ALL outbound RST; makes closed ports look filtered):")
				log.Printf("!   sudo iptables -A OUTPUT -p tcp --tcp-flags RST RST -j DROP")
			}
		}
		select {}
	} else {
		log.Printf("Running as CLIENT (SOCKS5 on %s)", *socksAddr)
		socks5Server := socks5.NewSOCKS5Server(*socksAddr, tun)
		log.Fatal(socks5Server.Start())
	}
}
