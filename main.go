package main

import (
	"context"
	"flag"
	"fmt"
	_ "github.com/wlynxg/anet"
	"log"
	"os"
	"os/signal"
	"strconv"
	"strings"
	"syscall"
	"universal-bypass-tool/nodeagent"
	"universal-bypass-tool/socks5"
	"universal-bypass-tool/transport"
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
	//os.Setenv("GODEBUG", "netdns=go")
	fmt.Print("written by p1neappleXpress\n")

	exitNode := flag.Bool("exit-node", false, "Run as exit node (needs root)")
	client := flag.Bool("client", false, "Run as client")
	debug := flag.Bool("debug", false, "Enable verbose debug logging")
	socksAddr := flag.String("socks5", ":1080", "SOCKS5 address")
	transportType := flag.String("transport", "yandex", "Transport type (yandex, google, custom)")
	managed := flag.Bool("managed", false, "Exit node only: fetch active keys from a controlplane instance instead of a single --url")
	controlURL := flag.String("control-url", "", "Managed mode: base URL of the openflux-control service")
	nodeToken := flag.String("node-token", "", "Managed mode: this node's bearer token from controlplane")
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

	if *managed {
		if !*exitNode {
			log.Fatalf("--managed is only valid together with --exit-node")
		}
		if *controlURL == "" || *nodeToken == "" {
			log.Fatalf("--managed requires --control-url and --node-token")
		}

		log.Printf("=== Universal Bypass Tool ===")
		log.Printf("Mode: EXIT NODE (managed, control=%s)", *controlURL)

		ctx, stop := signal.NotifyContext(context.Background(), syscall.SIGINT, syscall.SIGTERM)
		defer stop()

		orch := nodeagent.NewOrchestrator(nodeagent.DefaultConfig(*controlURL, *nodeToken))
		orch.Run(ctx)
		return
	}

	log.Printf("=== Universal Bypass Tool ===")
	log.Printf("Mode: %s", map[bool]string{true: "EXIT NODE", false: "CLIENT"}[*exitNode])
	log.Printf("Transport: %s", *transportType)

	config := transport.DefaultConfig()
	var trans transport.Transport

	switch *transportType {
	case "yandex":
		trans = transport.NewCompressedTransport(yandex.NewYandexDocsTransport(globalDocUrl, config))
	case "volga":
		trans = transport.NewCompressedTransport(yandex.NewYandexVolgaTransport(globalDocUrl, config))
	case "oneme":
		uidint, _ := strconv.ParseInt(maxUid, 10, 64)
		trans = transport.NewCompressedTransport(oneme.NewOneMeTransport(*exitNode, maxToken, uidint, config))
	case "yandex_multistream":
		urls := strings.Split(*docUrls, ",")
		if len(urls) < 2 {
			log.Fatalf("--transport yandex_multistream requires --urls with 2+ comma-separated doc URLs")
		}
		streams := make([]transport.Transport, len(urls))
		for i, url := range urls {
			streams[i] = transport.NewCompressedTransport(yandex.NewYandexDocsTransport(strings.TrimSpace(url), config))
		}
		trans = transport.NewMultiStreamTransport(streams)
	default:
		log.Fatalf("Unknown transport type: %s", *transportType)
	}

	if err := trans.Start(); err != nil {
		log.Fatalf("Failed to start transport: %v", err)
	}

	tun := tunnel.NewTCPTunnel(trans, *exitNode)

	if *exitNode {
		log.Printf("Running as EXIT NODE (needs root for raw socket)")
		log.Printf("! Run: sudo iptables -A OUTPUT -p tcp --tcp-flags RST RST -j DROP")
		select {}
	} else {
		log.Printf("Running as CLIENT (SOCKS5 on %s)", *socksAddr)
		socks5Server := socks5.NewSOCKS5Server(*socksAddr, tun)
		log.Fatal(socks5Server.Start())
	}
}
