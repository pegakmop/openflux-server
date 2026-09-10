package mobile

import "testing"

func TestResolveConfigManualYandex(t *testing.T) {
	docURL, err := resolveConfig(Config{Mode: "manual", DocURL: "https://docs.yandex.ru/x"})
	if err != nil {
		t.Fatalf("resolveConfig: %v", err)
	}
	if docURL != "https://docs.yandex.ru/x" {
		t.Errorf("docURL = %q", docURL)
	}
}

func TestResolveConfigManualMissingDocURL(t *testing.T) {
	if _, err := resolveConfig(Config{Mode: "manual", Transport: "yandex"}); err == nil {
		t.Fatalf("expected an error when doc_url is missing")
	}
}

func TestResolveConfigUnsupportedTransport(t *testing.T) {
	if _, err := resolveConfig(Config{Mode: "manual", Transport: "oneme", DocURL: "https://x"}); err == nil {
		t.Fatalf("expected an error for an unsupported transport")
	}
}

func TestResolveConfigKeyModeMissingFields(t *testing.T) {
	if _, err := resolveConfig(Config{Mode: "key"}); err == nil {
		t.Fatalf("expected an error when control_url/key_token are missing")
	}
}

func TestResolveConfigUnknownMode(t *testing.T) {
	if _, err := resolveConfig(Config{Mode: "telepathy"}); err == nil {
		t.Fatalf("expected an error for an unknown mode")
	}
}
