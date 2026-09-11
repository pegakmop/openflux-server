package api

import (
	"encoding/base64"
	"encoding/json"
	"strings"
	"testing"
)

func TestBuildDeepLinkEmptyWithoutPublicBaseURL(t *testing.T) {
	if got := buildDeepLink("", "label", "tok", "https://docs.yandex.ru/x", "yandex"); got != "" {
		t.Errorf("buildDeepLink with no public base URL = %q, want empty", got)
	}
}

func TestBuildDeepLinkRoundTrips(t *testing.T) {
	link := buildDeepLink("https://example.com", "user-42", "of_key_abc", "https://docs.yandex.ru/x", "yandex")

	const prefix = "openflux://import?data="
	if !strings.HasPrefix(link, prefix) {
		t.Fatalf("link = %q, want prefix %q", link, prefix)
	}

	// Must match the Android app's Base64.URL_SAFE|NO_WRAP|NO_PADDING
	// exactly (see ProfileDeepLink.kt) - RawURLEncoding is Go's equivalent.
	data := strings.TrimPrefix(link, prefix)
	decoded, err := base64.RawURLEncoding.DecodeString(data)
	if err != nil {
		t.Fatalf("payload is not valid unpadded base64url: %v", err)
	}

	var payload map[string]any
	if err := json.Unmarshal(decoded, &payload); err != nil {
		t.Fatalf("payload is not valid JSON: %v", err)
	}

	want := map[string]any{
		"name":        "user-42",
		"mode":        "key",
		"control_url": "https://example.com",
		"key_token":   "of_key_abc",
		"doc_url":     "https://docs.yandex.ru/x",
		"transport":   "yandex",
	}
	for k, v := range want {
		if payload[k] != v {
			t.Errorf("payload[%q] = %v, want %v", k, payload[k], v)
		}
	}
}
