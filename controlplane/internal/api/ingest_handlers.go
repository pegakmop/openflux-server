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

	// Tracks doc_urls claimed earlier in this same batch, since
	// HasEnabledKeyWithDocURL only sees keys already committed to the
	// database - two entries in one bulk-import request sharing a doc_url
	// would otherwise both pass that check before either is inserted.
	docURLsInBatch := make(map[string]bool, len(reqs))

	created := make([]createdKey, 0, len(reqs))
	for _, req := range reqs {
		if req.DocURL == "" {
			writeError(w, http.StatusBadRequest, "doc_url is required for every key")
			return
		}
		// Two enabled keys sharing one Yandex Docs document sit in the same
		// broadcast room - see HasEnabledKeyWithDocURL's doc comment - and a
		// new key is enabled immediately (enabled defaults to true), so this
		// has to be checked at creation time, not just when re-enabling one.
		if docURLsInBatch[req.DocURL] {
			writeError(w, http.StatusConflict, "doc_url is used by more than one key in this request - each key needs its own Yandex Docs document")
			return
		}
		inUse, err := a.Store.HasEnabledKeyWithDocURL(r.Context(), req.DocURL, "")
		if err != nil {
			writeInternalError(w, r, "check doc_url failed", err)
			return
		}
		if inUse {
			writeError(w, http.StatusConflict, "doc_url is already used by another enabled key - each key needs its own Yandex Docs document")
			return
		}
		docURLsInBatch[req.DocURL] = true
		if req.Transport == "" {
			req.Transport = "yandex"
		}
		ownerRef := req.OwnerRef
		if forcedOwnerRef != "" {
			ownerRef = forcedOwnerRef
		}

		token, err := auth.GenerateToken("key")
		if err != nil {
			writeInternalError(w, r, "token generation failed", err)
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
			writeInternalError(w, r, "create key failed", err)
			return
		}

		created = append(created, createdKey{
			ID:       k.ID,
			Token:    token,
			DeepLink: buildDeepLink(a.Config.PublicBaseURL, req.Label, token, req.DocURL, req.Transport),
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
