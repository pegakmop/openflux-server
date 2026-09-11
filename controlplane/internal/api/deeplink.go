package api

import (
	"encoding/base64"
	"encoding/json"
)

// buildDeepLink returns an openflux://import deep link for a freshly created
// key, in the app's ProfileDeepLink.kt JSON schema (see openflux-app's
// data/ProfileDeepLink.kt and its README) - tapping it opens the Android app
// straight to a prefilled "key" mode profile. Only the fields that matter
// for key mode are included; the app's decoder falls back to its own
// defaults (mtu, dns_upstream, ...) for everything else via
// optString/optInt/optBoolean, so there is no need to duplicate those
// defaults here.
//
// Returns "" if publicBaseURL is empty (CONTROLPLANE_PUBLIC_URL unset) - a
// deep link whose control_url doesn't actually resolve to this server isn't
// useful to hand out, so callers should treat "" as "omit it" rather than
// including a broken link.
func buildDeepLink(publicBaseURL, label, keyToken string) string {
	if publicBaseURL == "" {
		return ""
	}
	payload := map[string]any{
		"name":        label,
		"mode":        "key",
		"control_url": publicBaseURL,
		"key_token":   keyToken,
	}
	data, err := json.Marshal(payload)
	if err != nil {
		return ""
	}
	return "openflux://import?data=" + base64.RawURLEncoding.EncodeToString(data)
}
