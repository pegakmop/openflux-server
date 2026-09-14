package nodeagent

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"time"
)

// ControlClient talks to the openflux-control HTTP API on behalf of one
// exit-node process, using that node's bearer token.
type ControlClient struct {
	baseURL    string
	nodeToken  string
	httpClient *http.Client
}

func NewControlClient(baseURL, nodeToken string) *ControlClient {
	return &ControlClient{
		baseURL:   baseURL,
		nodeToken: nodeToken,
		httpClient: &http.Client{
			Timeout: 15 * time.Second,
		},
	}
}

type RemoteKey struct {
	ID     string `json:"id"`
	DocURL string `json:"doc_url"`
	// DocURLs is set instead of DocURL for transport "yandex_multistream"
	// (2+ URLs) - see transport.NewMultiStreamTransport.
	DocURLs           []string `json:"doc_urls,omitempty"`
	Transport         string   `json:"transport"`
	TrafficLimitBytes *int64   `json:"traffic_limit_bytes,omitempty"`
	BytesUsedTotal    int64    `json:"bytes_used_total"`
	// Token, when set, is the raw key token - the client uses it as the
	// end-to-end encryption key (see transport.NewEncryptedTransport), so
	// this worker needs the same value to derive matching keys. Empty for
	// a key created before that existed; the worker just runs unencrypted.
	Token string `json:"token,omitempty"`
}

func (c *ControlClient) ListKeys(ctx context.Context) ([]RemoteKey, error) {
	var out []RemoteKey
	if err := c.do(ctx, http.MethodGet, "/v1/nodes/keys", nil, &out); err != nil {
		return nil, err
	}
	return out, nil
}

type UsageDelta struct {
	KeyID              string `json:"key_id"`
	BytesSentDelta     int64  `json:"bytes_sent_delta"`
	BytesReceivedDelta int64  `json:"bytes_received_delta"`
}

type reportUsageRequest struct {
	Deltas []UsageDelta `json:"deltas"`
}

type reportUsageResponse struct {
	DisabledNow []string `json:"disabled_now"`
}

// ReportUsage sends batched traffic deltas and returns the keys the control
// plane disabled as a direct result (over quota), so the caller can tear
// those workers down immediately instead of waiting for the next poll.
func (c *ControlClient) ReportUsage(ctx context.Context, deltas []UsageDelta) ([]string, error) {
	if len(deltas) == 0 {
		return nil, nil
	}
	var out reportUsageResponse
	if err := c.do(ctx, http.MethodPost, "/v1/nodes/me/usage", reportUsageRequest{Deltas: deltas}, &out); err != nil {
		return nil, err
	}
	return out.DisabledNow, nil
}

func (c *ControlClient) Heartbeat(ctx context.Context) error {
	return c.do(ctx, http.MethodPost, "/v1/nodes/me/heartbeat", nil, nil)
}

func (c *ControlClient) do(ctx context.Context, method, path string, body, out interface{}) error {
	var reader io.Reader
	if body != nil {
		b, err := json.Marshal(body)
		if err != nil {
			return fmt.Errorf("marshal request: %w", err)
		}
		reader = bytes.NewReader(b)
	}

	req, err := http.NewRequestWithContext(ctx, method, c.baseURL+path, reader)
	if err != nil {
		return fmt.Errorf("build request: %w", err)
	}
	req.Header.Set("Authorization", "Bearer "+c.nodeToken)
	if body != nil {
		req.Header.Set("Content-Type", "application/json")
	}

	resp, err := c.httpClient.Do(req)
	if err != nil {
		return fmt.Errorf("request %s %s: %w", method, path, err)
	}
	defer resp.Body.Close()

	if resp.StatusCode >= 300 {
		respBody, _ := io.ReadAll(io.LimitReader(resp.Body, 4096))
		return fmt.Errorf("%s %s: status %d: %s", method, path, resp.StatusCode, string(respBody))
	}

	if out == nil {
		return nil
	}
	if err := json.NewDecoder(resp.Body).Decode(out); err != nil {
		return fmt.Errorf("decode response for %s %s: %w", method, path, err)
	}
	return nil
}
