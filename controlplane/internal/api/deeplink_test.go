package api

import (
	"encoding/base64"
	"encoding/json"
	"strings"
	"testing"
)

func TestBuildDeepLinkEmptyWithoutPublicBaseURL(t *testing.T) {
	if got := buildDeepLink("", "label", "tok", "https://docs.yandex.ru/x", nil, "yandex", false); got != "" {
		t.Errorf("buildDeepLink with no public base URL = %q, want empty", got)
	}
}

func TestBuildDeepLinkRoundTrips(t *testing.T) {
	link := buildDeepLink("https://example.com", "user-42", "of_key_abc", "https://docs.yandex.ru/x", nil, "yandex", false)

	const prefix = "openflux://import?data="
	if !strings.HasPrefix(link, prefix) {
		t.Fatalf("link = %q, want prefix %q", link, prefix)
	}

	// Must match the Android app's Base64.URL_SAFE|NO_WRAP|NO_PADDING exactly - RawURLEncoding is Go's equivalent.
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
	if _, present := payload["doc_urls"]; present {
		t.Errorf("payload has doc_urls = %v, want it omitted for a non-multistream key", payload["doc_urls"])
	}
	if _, present := payload["e2e_encryption"]; present {
		t.Errorf("payload has e2e_encryption = %v, want it omitted when off", payload["e2e_encryption"])
	}
}

func TestBuildDeepLinkIncludesDocURLsForMultistream(t *testing.T) {
	urls := []string{"https://docs.yandex.ru/a", "https://docs.yandex.ru/b"}
	link := buildDeepLink("https://example.com", "user-42", "of_key_abc", "", urls, "yandex_multistream", false)

	data := strings.TrimPrefix(link, "openflux://import?data=")
	decoded, err := base64.RawURLEncoding.DecodeString(data)
	if err != nil {
		t.Fatalf("payload is not valid unpadded base64url: %v", err)
	}
	var payload map[string]any
	if err := json.Unmarshal(decoded, &payload); err != nil {
		t.Fatalf("payload is not valid JSON: %v", err)
	}

	gotURLs, ok := payload["doc_urls"].([]any)
	if !ok || len(gotURLs) != len(urls) {
		t.Fatalf("payload[doc_urls] = %v, want %v", payload["doc_urls"], urls)
	}
	for i, u := range urls {
		if gotURLs[i] != u {
			t.Errorf("payload[doc_urls][%d] = %v, want %v", i, gotURLs[i], u)
		}
	}
}

func TestBuildDeepLinkIncludesE2EEncryptionWhenOn(t *testing.T) {
	link := buildDeepLink("https://example.com", "user-42", "of_key_abc", "https://docs.yandex.ru/x", nil, "yandex", true)

	data := strings.TrimPrefix(link, "openflux://import?data=")
	decoded, err := base64.RawURLEncoding.DecodeString(data)
	if err != nil {
		t.Fatalf("payload is not valid unpadded base64url: %v", err)
	}
	var payload map[string]any
	if err := json.Unmarshal(decoded, &payload); err != nil {
		t.Fatalf("payload is not valid JSON: %v", err)
	}
	if payload["e2e_encryption"] != true {
		t.Errorf("payload[e2e_encryption] = %v, want true", payload["e2e_encryption"])
	}
}
