package api

import (
	"encoding/base64"
	"encoding/json"
)

// buildDeepLink embeds doc_url/transport directly so the app never calls /v1/resolve, which goes to a bare IP with none of the tunnel's disguise; returns "" if publicBaseURL is unset.
func buildDeepLink(publicBaseURL, label, keyToken, docURL string, docURLs []string, transportName string, e2eEncryption bool) string {
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
	if len(docURLs) > 0 {
		payload["doc_urls"] = docURLs
	}
	if e2eEncryption {
		payload["e2e_encryption"] = true
	}
	data, err := json.Marshal(payload)
	if err != nil {
		return ""
	}
	return "openflux://import?data=" + base64.RawURLEncoding.EncodeToString(data)
}
