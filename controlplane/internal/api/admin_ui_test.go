package api

import (
	"net/http/httptest"
	"strings"
	"testing"
)

func TestHandleAdminUIServesEmbeddedHTML(t *testing.T) {
	for _, path := range []string{"/admin/", "/admin/index.html"} {
		req := httptest.NewRequest("GET", path, nil)
		rec := httptest.NewRecorder()

		handleAdminUI(rec, req)

		if rec.Code != 200 {
			t.Fatalf("%s: status = %d, want 200", path, rec.Code)
		}
		ct := rec.Header().Get("Content-Type")
		if !strings.HasPrefix(ct, "text/html") {
			t.Errorf("%s: Content-Type = %q, want text/html prefix", path, ct)
		}
		body := rec.Body.String()
		if !strings.Contains(body, "OpenFlux Control Plane") {
			t.Errorf("%s: body missing expected title text", path)
		}
		if !strings.Contains(body, "/v1/admin/nodes") {
			t.Errorf("%s: body missing expected API call", path)
		}
	}
}
