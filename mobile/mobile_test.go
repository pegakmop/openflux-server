package mobile

import (
	"testing"

	"universal-bypass-tool/transport"
	"universal-bypass-tool/transport/oneme"
	"universal-bypass-tool/transport/yandex"
)

// A plain "yandex" profile with no E2E encryption gets self-compression
// enabled instead of an external transport.CompressedTransport wrapper (see
// wrapYandex) - the returned type is the bare *yandex.YandexDocsTransport
// either way, since self-compress mode doesn't wrap it in anything.
func TestBuildTransportManualYandex(t *testing.T) {
	trans, err := buildTransport(Config{Mode: "manual", DocURL: "https://docs.yandex.ru/x"}, transport.DefaultConfig())
	if err != nil {
		t.Fatalf("buildTransport: %v", err)
	}
	if _, ok := trans.(*yandex.YandexDocsTransport); !ok {
		t.Errorf("got %T, want *yandex.YandexDocsTransport", trans)
	}
}

// A "yandex" profile with E2E encryption gets encrypted self-compression
// (see wrapYandex), still with no external wrapping - same bare
// *yandex.YandexDocsTransport type. This only guards that buildTransport
// wires E2E config through rather than silently ignoring it.
func TestBuildTransportManualYandexWithE2EEncryption(t *testing.T) {
	cfg := Config{
		Mode:          "manual",
		DocURL:        "https://docs.yandex.ru/x",
		KeyToken:      "shared-secret-token",
		E2EEncryption: true,
	}
	trans, err := buildTransport(cfg, transport.DefaultConfig())
	if err != nil {
		t.Fatalf("buildTransport: %v", err)
	}
	if _, ok := trans.(*yandex.YandexDocsTransport); !ok {
		t.Errorf("got %T, want *yandex.YandexDocsTransport", trans)
	}
}

func TestBuildTransportManualMissingDocURL(t *testing.T) {
	if _, err := buildTransport(Config{Mode: "manual", Transport: "yandex"}, transport.DefaultConfig()); err == nil {
		t.Fatalf("expected an error when doc_url is missing")
	}
}

// volga doesn't manage its own compression (see wrapGeneric) - it's always
// wrapped in transport.CompressedTransport, same as before this feature
// existed.
func TestBuildTransportManualVolga(t *testing.T) {
	trans, err := buildTransport(Config{Mode: "manual", Transport: "volga", DocURL: "https://docs.yandex.ru/x"}, transport.DefaultConfig())
	if err != nil {
		t.Fatalf("buildTransport: %v", err)
	}
	compressed, ok := trans.(*transport.CompressedTransport)
	if !ok {
		t.Fatalf("got %T, want *transport.CompressedTransport", trans)
	}
	if _, ok := compressed.Transport.(*yandex.YandexVolgaTransport); !ok {
		t.Errorf("inner transport is %T, want *yandex.YandexVolgaTransport", compressed.Transport)
	}
}

func TestBuildTransportVolgaMissingDocURL(t *testing.T) {
	if _, err := buildTransport(Config{Mode: "manual", Transport: "volga"}, transport.DefaultConfig()); err == nil {
		t.Fatalf("expected an error when doc_url is missing")
	}
}

func TestBuildTransportManualMax(t *testing.T) {
	trans, err := buildTransport(
		Config{Mode: "manual", Transport: "max", MaxToken: "tok", MaxUID: 12345},
		transport.DefaultConfig(),
	)
	if err != nil {
		t.Fatalf("buildTransport: %v", err)
	}
	compressed, ok := trans.(*transport.CompressedTransport)
	if !ok {
		t.Fatalf("got %T, want *transport.CompressedTransport", trans)
	}
	if _, ok := compressed.Transport.(*oneme.OneMeTransport); !ok {
		t.Errorf("inner transport is %T, want *oneme.OneMeTransport", compressed.Transport)
	}
}

func TestBuildTransportManualMaxMissingFields(t *testing.T) {
	cases := []Config{
		{Mode: "manual", Transport: "max", MaxUID: 12345},   // no token
		{Mode: "manual", Transport: "max", MaxToken: "tok"}, // no uid
	}
	for _, cfg := range cases {
		if _, err := buildTransport(cfg, transport.DefaultConfig()); err == nil {
			t.Errorf("expected an error for %+v", cfg)
		}
	}
}

func TestBuildTransportUnsupportedTransport(t *testing.T) {
	if _, err := buildTransport(Config{Mode: "manual", Transport: "carrier-pigeon", DocURL: "https://x"}, transport.DefaultConfig()); err == nil {
		t.Fatalf("expected an error for an unsupported transport")
	}
}

func TestBuildTransportUnknownMode(t *testing.T) {
	if _, err := buildTransport(Config{Mode: "telepathy"}, transport.DefaultConfig()); err == nil {
		t.Fatalf("expected an error for an unknown mode")
	}
}

func TestBuildTransportRejectsRemovedKeyMode(t *testing.T) {
	// "key" mode used to make StartTunnel itself resolve a controlplane
	// token into a doc_url over a live, unshielded HTTPS request - removed
	// because that request had no disguise and was trivial to block. Any
	// caller still sending it should get a clear error, not silent misuse.
	if _, err := buildTransport(Config{Mode: "key", DocURL: "https://x"}, transport.DefaultConfig()); err == nil {
		t.Fatalf(`expected an error for the removed "key" mode`)
	}
}

func TestBuildTransportMultiStream(t *testing.T) {
	// KeyToken set but E2EEncryption left false: must still build successfully
	// without encrypting - a bare KeyToken alone must never turn encryption on.
	cfg := Config{
		Mode:      "manual",
		Transport: "yandex_multistream",
		DocURLs:   []string{"https://docs.yandex.ru/a", "https://docs.yandex.ru/b", "https://docs.yandex.ru/c"},
		KeyToken:  "shared-secret-token",
	}
	trans, err := buildTransport(cfg, transport.DefaultConfig())
	if err != nil {
		t.Fatalf("buildTransport: %v", err)
	}
	if _, ok := trans.(*transport.MultiStreamTransport); !ok {
		t.Errorf("got %T, want *transport.MultiStreamTransport", trans)
	}
}

func TestBuildTransportMultiStreamWithE2EEncryption(t *testing.T) {
	cfg := Config{
		Mode:          "manual",
		Transport:     "yandex_multistream",
		DocURLs:       []string{"https://docs.yandex.ru/a", "https://docs.yandex.ru/b"},
		KeyToken:      "shared-secret-token",
		E2EEncryption: true,
	}
	trans, err := buildTransport(cfg, transport.DefaultConfig())
	if err != nil {
		t.Fatalf("buildTransport: %v", err)
	}
	if _, ok := trans.(*transport.MultiStreamTransport); !ok {
		t.Errorf("got %T, want *transport.MultiStreamTransport", trans)
	}
}

func TestBuildTransportMultiStreamRequiresAtLeastTwoURLs(t *testing.T) {
	cfg := Config{Mode: "manual", Transport: "yandex_multistream", DocURLs: []string{"https://docs.yandex.ru/a"}}
	if _, err := buildTransport(cfg, transport.DefaultConfig()); err == nil {
		t.Fatalf("expected an error with fewer than 2 doc_urls")
	}
}

// StartSocks5Proxy doesn't actually need a reachable doc_url to exercise its
// own lifecycle/mutual-exclusion logic: YandexDocsTransport.Start() launches
// its connection attempt in a background goroutine and returns immediately
// regardless of whether the URL is real (see yandex.go's Start/connectToDoc).
func TestStartSocks5ProxyLifecycleAndMutualExclusion(t *testing.T) {
	cfg := `{"mode":"manual","transport":"yandex","doc_url":"http://127.0.0.1:1"}`

	if err := StartSocks5Proxy(cfg, "127.0.0.1:0", nil); err != nil {
		t.Fatalf("StartSocks5Proxy: %v", err)
	}
	defer StopSocks5Proxy()

	if err := StartSocks5Proxy(cfg, "127.0.0.1:0", nil); err == nil {
		t.Fatal("expected an error starting a second SOCKS5 proxy while one is already running")
	}
	if err := StartTunnel(0, cfg, nil, nil); err == nil {
		t.Fatal("expected an error starting a tunnel while a SOCKS5 proxy is running")
	}

	if err := StopSocks5Proxy(); err != nil {
		t.Fatalf("StopSocks5Proxy: %v", err)
	}
	if err := StopSocks5Proxy(); err != nil {
		t.Fatalf("a second StopSocks5Proxy should be a harmless no-op, got: %v", err)
	}
}
