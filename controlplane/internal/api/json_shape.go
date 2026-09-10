package api

import (
	"bytes"
	"fmt"
	"io"
	"net/http"
)

type jsonShape struct {
	isArray bool
}

const maxBodyBytes = 1 << 20 // 1 MiB is generous for a batch of key-ingest objects

// peekJSONShape reads the request body (bounded, since it comes from an
// authenticated but still external caller) to see whether it's a JSON array
// or object, then rewinds r.Body so a later json.Decode sees the same bytes.
func peekJSONShape(r *http.Request) (jsonShape, error) {
	limited := io.LimitReader(r.Body, maxBodyBytes+1)
	data, err := io.ReadAll(limited)
	if err != nil {
		return jsonShape{}, fmt.Errorf("read body: %w", err)
	}
	if len(data) > maxBodyBytes {
		return jsonShape{}, fmt.Errorf("request body too large")
	}

	r.Body = io.NopCloser(bytes.NewReader(data))

	trimmed := bytes.TrimLeft(data, " \t\r\n")
	return jsonShape{isArray: len(trimmed) > 0 && trimmed[0] == '['}, nil
}
