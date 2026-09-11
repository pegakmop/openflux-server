package mobile

import (
	"testing"

	"universal-bypass-tool/transport"
	"universal-bypass-tool/transport/oneme"
	"universal-bypass-tool/transport/yandex"
)

func TestBuildTransportManualYandex(t *testing.T) {
	trans, err := buildTransport(Config{Mode: "manual", DocURL: "https://docs.yandex.ru/x"}, transport.DefaultConfig())
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

func TestBuildTransportManualMax(t *testing.T) {
	trans, err := buildTransport(
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

func TestBuildTransportKeyModeMissingFields(t *testing.T) {
	if _, err := buildTransport(Config{Mode: "key"}, transport.DefaultConfig()); err == nil {
		t.Fatalf("expected an error when control_url/key_token are missing")
	}
}

func TestBuildTransportUnknownMode(t *testing.T) {
	if _, err := buildTransport(Config{Mode: "telepathy"}, transport.DefaultConfig()); err == nil {
		t.Fatalf("expected an error for an unknown mode")
	}
}
