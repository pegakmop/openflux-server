package mobile

import (
	"testing"

	"universal-bypass-tool/transport"
	"universal-bypass-tool/transport/oneme"
	"universal-bypass-tool/transport/yandex"
)

func TestBuildTransportManualYandex(t *testing.T) {
	trans, wrapped, err := buildTransport(Config{Mode: "manual", DocURL: "https://docs.yandex.ru/x"}, transport.DefaultConfig())
	if err != nil {
		t.Fatalf("buildTransport: %v", err)
	}
	if wrapped {
		t.Errorf("plain yandex transport should not report itself as already wrapped")
	}
	if _, ok := trans.(*yandex.YandexDocsTransport); !ok {
		t.Errorf("got %T, want *yandex.YandexDocsTransport", trans)
	}
}

func TestBuildTransportManualMissingDocURL(t *testing.T) {
	if _, _, err := buildTransport(Config{Mode: "manual", Transport: "yandex"}, transport.DefaultConfig()); err == nil {
		t.Fatalf("expected an error when doc_url is missing")
	}
}

func TestBuildTransportManualVolga(t *testing.T) {
	trans, _, err := buildTransport(Config{Mode: "manual", Transport: "volga", DocURL: "https://docs.yandex.ru/x"}, transport.DefaultConfig())
	if err != nil {
		t.Fatalf("buildTransport: %v", err)
	}
	if _, ok := trans.(*yandex.YandexVolgaTransport); !ok {
		t.Errorf("got %T, want *yandex.YandexVolgaTransport", trans)
	}
}

func TestBuildTransportVolgaMissingDocURL(t *testing.T) {
	if _, _, err := buildTransport(Config{Mode: "manual", Transport: "volga"}, transport.DefaultConfig()); err == nil {
		t.Fatalf("expected an error when doc_url is missing")
	}
}

func TestBuildTransportManualMax(t *testing.T) {
	trans, _, err := buildTransport(
		Config{Mode: "manual", Transport: "max", MaxToken: "tok", MaxUID: 12345},
		transport.DefaultConfig(),
	)
	if err != nil {
		t.Fatalf("buildTransport: %v", err)
	}
	if _, ok := trans.(*oneme.OneMeTransport); !ok {
		t.Errorf("got %T, want *oneme.OneMeTransport", trans)
	}
}

func TestBuildTransportManualMaxMissingFields(t *testing.T) {
	cases := []Config{
		{Mode: "manual", Transport: "max", MaxUID: 12345},   // no token
		{Mode: "manual", Transport: "max", MaxToken: "tok"}, // no uid
	}
	for _, cfg := range cases {
		if _, _, err := buildTransport(cfg, transport.DefaultConfig()); err == nil {
			t.Errorf("expected an error for %+v", cfg)
		}
	}
}

func TestBuildTransportUnsupportedTransport(t *testing.T) {
	if _, _, err := buildTransport(Config{Mode: "manual", Transport: "carrier-pigeon", DocURL: "https://x"}, transport.DefaultConfig()); err == nil {
		t.Fatalf("expected an error for an unsupported transport")
	}
}

func TestBuildTransportUnknownMode(t *testing.T) {
	if _, _, err := buildTransport(Config{Mode: "telepathy"}, transport.DefaultConfig()); err == nil {
		t.Fatalf("expected an error for an unknown mode")
	}
}

func TestBuildTransportRejectsRemovedKeyMode(t *testing.T) {
	// "key" mode used to make StartTunnel itself resolve a controlplane
	// token into a doc_url over a live, unshielded HTTPS request - removed
	// because that request had no disguise and was trivial to block. Any
	// caller still sending it should get a clear error, not silent misuse.
	if _, _, err := buildTransport(Config{Mode: "key", DocURL: "https://x"}, transport.DefaultConfig()); err == nil {
		t.Fatalf(`expected an error for the removed "key" mode`)
	}
}

func TestBuildTransportMultiStream(t *testing.T) {
	cfg := Config{
		Mode:      "manual",
		Transport: "yandex_multistream",
		DocURLs:   []string{"https://docs.yandex.ru/a", "https://docs.yandex.ru/b", "https://docs.yandex.ru/c"},
		KeyToken:  "shared-secret-token",
	}
	trans, wrapped, err := buildTransport(cfg, transport.DefaultConfig())
	if err != nil {
		t.Fatalf("buildTransport: %v", err)
	}
	if !wrapped {
		t.Errorf("yandex_multistream should report itself as already wrapped (it builds its own per-stream compression/encryption)")
	}
	if _, ok := trans.(*transport.MultiStreamTransport); !ok {
		t.Errorf("got %T, want *transport.MultiStreamTransport", trans)
	}
}

func TestBuildTransportMultiStreamRequiresAtLeastTwoURLs(t *testing.T) {
	cfg := Config{Mode: "manual", Transport: "yandex_multistream", DocURLs: []string{"https://docs.yandex.ru/a"}}
	if _, _, err := buildTransport(cfg, transport.DefaultConfig()); err == nil {
		t.Fatalf("expected an error with fewer than 2 doc_urls")
	}
}
