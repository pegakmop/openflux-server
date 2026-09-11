package api

import (
	"net/http"
	"time"

	"openflux-control/internal/auth"
	"openflux-control/internal/store"
)

type createKeyRequest struct {
	Label             string     `json:"label"`
	Transport         string     `json:"transport"`
	DocURL            string     `json:"doc_url"`
	TrafficLimitBytes *int64     `json:"traffic_limit_bytes"`
	OwnerRef          string     `json:"owner_ref"`
	ExpiresAt         *time.Time `json:"expires_at"`
}

type createdKey struct {
	ID       string `json:"id"`
	Token    string `json:"token"`
	DeepLink string `json:"deep_link,omitempty"`
}

// createKeyHandler is shared by the admin key-creation endpoint and the
// third-party ingestion endpoint. It accepts either a single object or an
// array for bulk import.
func createKeyHandler(a *App, w http.ResponseWriter, r *http.Request, forcedOwnerRef string) {
	var reqs []createKeyRequest

	var single createKeyRequest
	var batch []createKeyRequest

	body, err := peekJSONShape(r)
	if err != nil {
		writeError(w, http.StatusBadRequest, "invalid body")
		return
	}

	if body.isArray {
		if err := readJSON(r, &batch); err != nil {
			writeError(w, http.StatusBadRequest, "invalid body")
			return
		}
		reqs = batch
	} else {
		if err := readJSON(r, &single); err != nil {
			writeError(w, http.StatusBadRequest, "invalid body")
			return
		}
		reqs = []createKeyRequest{single}
	}

	if len(reqs) == 0 {
		writeError(w, http.StatusBadRequest, "at least one key is required")
		return
	}
	if len(reqs) > 200 {
		writeError(w, http.StatusBadRequest, "too many keys in one request (max 200)")
		return
	}

	created := make([]createdKey, 0, len(reqs))
	for _, req := range reqs {
		if req.DocURL == "" {
			writeError(w, http.StatusBadRequest, "doc_url is required for every key")
			return
		}
		if req.Transport == "" {
			req.Transport = "yandex"
		}
		ownerRef := req.OwnerRef
		if forcedOwnerRef != "" {
			ownerRef = forcedOwnerRef
		}

		token, err := auth.GenerateToken("key")
		if err != nil {
			writeError(w, http.StatusInternalServerError, "token generation failed")
			return
		}

		k, err := a.Store.CreateKey(r.Context(), store.CreateKeyParams{
			TokenHash:         a.Hasher.Hash(token),
			Label:             req.Label,
			Transport:         req.Transport,
			DocURL:            req.DocURL,
			TrafficLimitBytes: req.TrafficLimitBytes,
			OwnerRef:          ownerRef,
			ExpiresAt:         req.ExpiresAt,
		})
		if err != nil {
			writeError(w, http.StatusInternalServerError, "create key failed")
			return
		}

		created = append(created, createdKey{
			ID:       k.ID,
			Token:    token,
			DeepLink: buildDeepLink(a.Config.PublicBaseURL, req.Label, token),
		})
	}

	if len(created) == 1 {
		writeJSON(w, http.StatusCreated, created[0])
		return
	}
	writeJSON(w, http.StatusCreated, created)
}

func (a *App) handleIngestKeys(w http.ResponseWriter, r *http.Request) {
	it := ingestTokenFromContext(r.Context())
	if it.Scope != "keys:write" {
		writeError(w, http.StatusForbidden, "ingest token missing keys:write scope")
		return
	}
	createKeyHandler(a, w, r, "")
}
