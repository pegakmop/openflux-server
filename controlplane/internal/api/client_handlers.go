package api

import (
	"net/http"
	"time"

	"openflux-control/internal/auth"
	"openflux-control/internal/store"
)

type resolveResponse struct {
	Status            string `json:"status"`
	DocURL            string `json:"doc_url,omitempty"`
	Transport         string `json:"transport,omitempty"`
	BytesUsedTotal    int64  `json:"bytes_used_total"`
	TrafficLimitBytes *int64 `json:"traffic_limit_bytes,omitempty"`
}

// handleResolve lets a client (the future Android app, or the desktop CLI)
// turn its key token into connection details and a status, instead of the
// operator handing out a raw Yandex Docs URL by hand. It is rate-limited per
// IP since the token is presented directly here.
func (a *App) handleResolve(w http.ResponseWriter, r *http.Request) {
	token, ok := auth.ExtractBearer(r)
	if !ok {
		writeError(w, http.StatusUnauthorized, "missing bearer token")
		return
	}

	k, err := a.Store.GetKeyByTokenHash(r.Context(), a.Hasher.Hash(token))
	if err == store.ErrNotFound {
		writeJSON(w, http.StatusOK, resolveResponse{Status: "not_found"})
		return
	}
	if err != nil {
		writeError(w, http.StatusInternalServerError, "lookup failed")
		return
	}

	status := k.Status(time.Now())
	resp := resolveResponse{
		Status:            status,
		BytesUsedTotal:    k.BytesUsedTotal(),
		TrafficLimitBytes: k.TrafficLimitBytes,
	}
	if status == "active" {
		resp.DocURL = k.DocURL
		resp.Transport = k.Transport
	}

	writeJSON(w, http.StatusOK, resp)
}
