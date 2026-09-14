package api

import (
	"encoding/base64"
	"encoding/json"
)

// buildDeepLink returns an openflux://import deep link for a freshly created
// key, in the app's ProfileDeepLink.kt JSON schema (see openflux-app's
// data/ProfileDeepLink.kt and its README) - tapping it opens the Android app
// straight to a prefilled profile. doc_url/transport are embedded directly
// so the app never has to call this server's /v1/resolve to connect: that
// request goes straight to a bare IP with none of the tunnel's own
// disguise, and a network that already blocks direct access to this server
// (exactly the kind of network this tool exists for) would block it too.
// control_url/key_token are still included for the app's own optional,
// user-initiated "check key" status/quota lookup, but are no longer
// required to connect. The app's decoder falls back to its own defaults
// (mtu, dns_upstream, ...) for everything else via
// optString/optInt/optBoolean, so there is no need to duplicate those
// defaults here.
//
// Returns "" if publicBaseURL is empty (CONTROLPLANE_PUBLIC_URL unset) - a
// deep link whose control_url doesn't actually resolve to this server isn't
// useful to hand out, so callers should treat "" as "omit it" rather than
// including a broken link.
func buildDeepLink(publicBaseURL, label, keyToken, docURL string, docURLs []string, transportName string) string {
	if publicBaseURL == "" {
		return ""
	}
	payload := map[string]any{
		"name":        label,
		"mode":        "key",
		"control_url": publicBaseURL,
		"key_token":   keyToken,
		"doc_url":     docURL,
		"transport":   transportName,
	}
	// Only set for yandex_multistream - see model.Key.DocURLs. Omitted
	// (rather than an empty array) for every other transport, matching how
	// every other optional field here works: the app's decoder only reads
	// this key at all when present (org.openflux.app.data.ProfileDeepLink).
	if len(docURLs) > 0 {
		payload["doc_urls"] = docURLs
	}
	data, err := json.Marshal(payload)
	if err != nil {
		return ""
	}
	return "openflux://import?data=" + base64.RawURLEncoding.EncodeToString(data)
}
