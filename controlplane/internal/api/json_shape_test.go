package api

import (
	"io"
	"net/http/httptest"
	"strings"
	"testing"
)

func TestPeekJSONShape(t *testing.T) {
	cases := []struct {
		name string
		body string
		want bool
	}{
		{"object", `{"doc_url":"https://example"}`, false},
		{"array", `[{"doc_url":"https://example"}]`, true},
		{"array with leading whitespace", "  \n[{}]", true},
		{"object with leading whitespace", "  \n{}", false},
	}

	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			req := httptest.NewRequest("POST", "/v1/ingest/keys", strings.NewReader(c.body))

			shape, err := peekJSONShape(req)
			if err != nil {
				t.Fatalf("peekJSONShape: %v", err)
			}
			if shape.isArray != c.want {
				t.Errorf("isArray = %v, want %v", shape.isArray, c.want)
			}

			// The body must still be readable afterwards with the same content.
			rest, err := io.ReadAll(req.Body)
			if err != nil {
				t.Fatalf("read rewound body: %v", err)
			}
			if string(rest) != c.body {
				t.Errorf("body after peek = %q, want %q", rest, c.body)
			}
		})
	}
}

func TestPeekJSONShapeTooLarge(t *testing.T) {
	huge := strings.Repeat("a", maxBodyBytes+2)
	req := httptest.NewRequest("POST", "/v1/ingest/keys", strings.NewReader(huge))

	if _, err := peekJSONShape(req); err == nil {
		t.Fatalf("expected an error for an oversized body")
	}
}
