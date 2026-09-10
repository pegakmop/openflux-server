package mobile

import (
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"strings"
	"time"
)

var httpClient = &http.Client{Timeout: 15 * time.Second}

type resolveResult struct {
	Status            string `json:"status"`
	DocURL            string `json:"doc_url"`
	Transport         string `json:"transport"`
	BytesUsedTotal    int64  `json:"bytes_used_total"`
	TrafficLimitBytes *int64 `json:"traffic_limit_bytes"`
}

// ResolveKey calls controlplane's POST /v1/resolve for keyToken and returns
// the raw JSON response body verbatim, so the Kotlin side can show
// status/quota (e.g. before the user taps connect) without a tunnel running
// and without a second copy of the response schema living in Kotlin.
func ResolveKey(controlURL, keyToken string) (string, error) {
	body, _, err := resolveRaw(controlURL, keyToken)
	return body, err
}

func resolveRaw(controlURL, keyToken string) (string, resolveResult, error) {
	if controlURL == "" || keyToken == "" {
		return "", resolveResult{}, fmt.Errorf("control_url and key_token are required")
	}

	url := strings.TrimRight(controlURL, "/") + "/v1/resolve"
	req, err := http.NewRequest(http.MethodPost, url, nil)
	if err != nil {
		return "", resolveResult{}, fmt.Errorf("build resolve request: %w", err)
	}
	req.Header.Set("Authorization", "Bearer "+keyToken)

	resp, err := httpClient.Do(req)
	if err != nil {
		return "", resolveResult{}, fmt.Errorf("resolve request: %w", err)
	}
	defer resp.Body.Close()

	data, err := io.ReadAll(io.LimitReader(resp.Body, 1<<16))
	if err != nil {
		return "", resolveResult{}, fmt.Errorf("read resolve response: %w", err)
	}
	if resp.StatusCode >= 300 {
		return string(data), resolveResult{}, fmt.Errorf("resolve failed: status %d", resp.StatusCode)
	}

	var parsed resolveResult
	if err := json.Unmarshal(data, &parsed); err != nil {
		return string(data), resolveResult{}, fmt.Errorf("parse resolve response: %w", err)
	}
	return string(data), parsed, nil
}
