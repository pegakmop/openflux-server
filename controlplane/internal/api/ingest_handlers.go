package api

import (
	"net/http"
	"time"

	"openflux-control/internal/auth"
	"openflux-control/internal/store"
)

type createKeyRequest struct {
	Label     string `json:"label"`
	Transport string `json:"transport"`
	DocURL    string `json:"doc_url"`
	// DocURLs is required instead of DocURL when Transport is
	// "yandex_multistream" (2+ URLs) - see model.Key.DocURLs.
	DocURLs           []string   `json:"doc_urls"`
	TrafficLimitBytes *int64     `json:"traffic_limit_bytes"`
	OwnerRef          string     `json:"owner_ref"`
	ExpiresAt         *time.Time `json:"expires_at"`
}

const transportYandexMultistream = "yandex_multistream"

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

	// Tracks every URL (doc_url, or every entry of doc_urls) claimed earlier
	// in this same batch, since HasEnabledKeyWithAnyDocURL only sees keys
	// already committed to the database - two entries in one bulk-import
	// request sharing a URL would otherwise both pass that check before
	// either is inserted.
	urlsInBatch := make(map[string]bool, len(reqs))

	created := make([]createdKey, 0, len(reqs))
	for _, req := range reqs {
		if req.Transport == "" {
			req.Transport = "yandex"
		}

		var urls []string
		if req.Transport == transportYandexMultistream {
			if len(req.DocURLs) < 2 {
				writeError(w, http.StatusBadRequest, "doc_urls needs 2+ entries for transport=yandex_multistream")
				return
			}
			urls = req.DocURLs
			req.DocURL = "" // ignore a stray doc_url - doc_urls is authoritative for this transport
		} else {
			if req.DocURL == "" {
				writeError(w, http.StatusBadRequest, "doc_url is required for every key")
				return
			}
			urls = []string{req.DocURL}
			req.DocURLs = nil
		}

		// Two enabled keys sharing one Yandex Docs document sit in the same
		// broadcast room - see HasEnabledKeyWithAnyDocURL's doc comment -
		// and a new key is enabled immediately (enabled defaults to true),
		// so this has to be checked at creation time, not just when
		// re-enabling one.
		for _, u := range urls {
			if urlsInBatch[u] {
				writeError(w, http.StatusConflict, "doc_url is used by more than one key in this request - each key needs its own Yandex Docs document")
				return
			}
		}
		inUse, err := a.Store.HasEnabledKeyWithAnyDocURL(r.Context(), urls, "")
		if err != nil {
			writeInternalError(w, r, "check doc_url failed", err)
			return
		}
		if inUse {
			writeError(w, http.StatusConflict, "doc_url is already used by another enabled key - each key needs its own Yandex Docs document")
			return
		}
		for _, u := range urls {
			urlsInBatch[u] = true
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
		tokenEnc, err := a.Cipher.Encrypt(token)
		if err != nil {
			writeInternalError(w, r, "token encryption failed", err)
			return
		}

		k, err := a.Store.CreateKey(r.Context(), store.CreateKeyParams{
			TokenHash:         a.Hasher.Hash(token),
			TokenEnc:          tokenEnc,
			Label:             req.Label,
			Transport:         req.Transport,
			DocURL:            req.DocURL,
			DocURLs:           req.DocURLs,
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
			DeepLink: buildDeepLink(a.Config.PublicBaseURL, req.Label, token, req.DocURL, req.DocURLs, req.Transport),
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
