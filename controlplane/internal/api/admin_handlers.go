package api

import (
	"net/http"
	"strconv"

	"openflux-control/internal/auth"
	"openflux-control/internal/model"
	"openflux-control/internal/store"
)

type createNodeRequest struct {
	Name    string `json:"name"`
	MaxKeys int    `json:"max_keys"`
}

type createNodeResponse struct {
	ID      string `json:"id"`
	Name    string `json:"name"`
	MaxKeys int    `json:"max_keys"`
	Token   string `json:"token"`
}

func (a *App) handleCreateNode(w http.ResponseWriter, r *http.Request) {
	var req createNodeRequest
	if err := readJSON(r, &req); err != nil {
		writeError(w, http.StatusBadRequest, "invalid body")
		return
	}
	if req.Name == "" {
		writeError(w, http.StatusBadRequest, "name is required")
		return
	}
	if req.MaxKeys <= 0 {
		req.MaxKeys = 500
	}

	token, err := auth.GenerateToken("node")
	if err != nil {
		writeInternalError(w, r, "token generation failed", err)
		return
	}

	n, err := a.Store.CreateNode(r.Context(), req.Name, a.Hasher.Hash(token), req.MaxKeys)
	if err != nil {
		writeInternalError(w, r, "create node failed", err)
		return
	}

	writeJSON(w, http.StatusCreated, createNodeResponse{ID: n.ID, Name: n.Name, MaxKeys: n.MaxKeys, Token: token})
}

func (a *App) handleListNodes(w http.ResponseWriter, r *http.Request) {
	nodes, err := a.Store.ListNodes(r.Context())
	if err != nil {
		writeInternalError(w, r, "list nodes failed", err)
		return
	}
	if nodes == nil {
		nodes = []model.Node{}
	}
	writeJSON(w, http.StatusOK, nodes)
}

func (a *App) handleRotateNodeToken(w http.ResponseWriter, r *http.Request) {
	id := r.PathValue("id")

	token, err := auth.GenerateToken("node")
	if err != nil {
		writeInternalError(w, r, "token generation failed", err)
		return
	}

	if err := a.Store.RotateNodeSecret(r.Context(), id, a.Hasher.Hash(token)); err == store.ErrNotFound {
		writeError(w, http.StatusNotFound, "node not found")
		return
	} else if err != nil {
		writeInternalError(w, r, "rotate failed", err)
		return
	}

	writeJSON(w, http.StatusOK, map[string]string{"token": token})
}

type createIngestTokenRequest struct {
	Label string `json:"label"`
	Scope string `json:"scope"`
}

func (a *App) handleCreateIngestToken(w http.ResponseWriter, r *http.Request) {
	var req createIngestTokenRequest
	if err := readJSON(r, &req); err != nil {
		writeError(w, http.StatusBadRequest, "invalid body")
		return
	}
	if req.Scope == "" {
		req.Scope = "keys:write"
	}

	token, err := auth.GenerateToken("ingest")
	if err != nil {
		writeInternalError(w, r, "token generation failed", err)
		return
	}

	it, err := a.Store.CreateIngestToken(r.Context(), a.Hasher.Hash(token), req.Label, req.Scope)
	if err != nil {
		writeInternalError(w, r, "create ingest token failed", err)
		return
	}

	writeJSON(w, http.StatusCreated, map[string]interface{}{
		"id": it.ID, "label": it.Label, "scope": it.Scope, "token": token,
	})
}

func (a *App) handleListIngestTokens(w http.ResponseWriter, r *http.Request) {
	tokens, err := a.Store.ListIngestTokens(r.Context())
	if err != nil {
		writeInternalError(w, r, "list ingest tokens failed", err)
		return
	}
	if tokens == nil {
		tokens = []model.IngestToken{}
	}
	writeJSON(w, http.StatusOK, tokens)
}

func (a *App) handleSetIngestTokenEnabled(enabled bool) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		if err := a.Store.SetIngestTokenEnabled(r.Context(), r.PathValue("id"), enabled); err == store.ErrNotFound {
			writeError(w, http.StatusNotFound, "ingest token not found")
			return
		} else if err != nil {
			writeInternalError(w, r, "update ingest token failed", err)
			return
		}
		writeJSON(w, http.StatusOK, map[string]bool{"enabled": enabled})
	}
}

func (a *App) handleListKeys(w http.ResponseWriter, r *http.Request) {
	q := r.URL.Query()
	limit, _ := strconv.Atoi(q.Get("limit"))
	offset, _ := strconv.Atoi(q.Get("offset"))
	keys, err := a.Store.ListKeys(r.Context(), store.ListKeysFilter{
		OwnerRef: q.Get("owner_ref"),
		Limit:    limit,
		Offset:   offset,
	})
	if err != nil {
		writeInternalError(w, r, "list keys failed", err)
		return
	}
	if keys == nil {
		keys = []model.Key{}
	}
	writeJSON(w, http.StatusOK, keys)
}

func (a *App) handleCreateKey(w http.ResponseWriter, r *http.Request) {
	createKeyHandler(a, w, r, "")
}

type rotateKeyTokenResponse struct {
	Token    string `json:"token"`
	DeepLink string `json:"deep_link,omitempty"`
}

// handleRotateKeyToken issues a fresh token for an existing key, since the
// original one is only ever stored hashed - there's no other way to get a
// usable token (or the deep link built from it) for a key whose raw token
// from creation is already gone. Every other field is untouched.
func (a *App) handleRotateKeyToken(w http.ResponseWriter, r *http.Request) {
	id := r.PathValue("id")

	token, err := auth.GenerateToken("key")
	if err != nil {
		writeInternalError(w, r, "token generation failed", err)
		return
	}

	k, err := a.Store.RotateKeyToken(r.Context(), id, a.Hasher.Hash(token))
	if err == store.ErrNotFound {
		writeError(w, http.StatusNotFound, "key not found")
		return
	}
	if err != nil {
		writeInternalError(w, r, "rotate key token failed", err)
		return
	}

	writeJSON(w, http.StatusOK, rotateKeyTokenResponse{
		Token:    token,
		DeepLink: buildDeepLink(a.Config.PublicBaseURL, k.Label, token, k.DocURL, k.Transport),
	})
}

func (a *App) handleGetKey(w http.ResponseWriter, r *http.Request) {
	k, err := a.Store.GetKeyByID(r.Context(), r.PathValue("id"))
	if err == store.ErrNotFound {
		writeError(w, http.StatusNotFound, "key not found")
		return
	}
	if err != nil {
		writeInternalError(w, r, "get key failed", err)
		return
	}
	writeJSON(w, http.StatusOK, k)
}

type patchKeyRequest struct {
	TrafficLimitBytes *int64 `json:"traffic_limit_bytes"`
}

func (a *App) handlePatchKey(w http.ResponseWriter, r *http.Request) {
	var req patchKeyRequest
	if err := readJSON(r, &req); err != nil {
		writeError(w, http.StatusBadRequest, "invalid body")
		return
	}

	if err := a.Store.SetKeyTrafficLimit(r.Context(), r.PathValue("id"), req.TrafficLimitBytes); err == store.ErrNotFound {
		writeError(w, http.StatusNotFound, "key not found")
		return
	} else if err != nil {
		writeInternalError(w, r, "update key failed", err)
		return
	}

	writeJSON(w, http.StatusOK, map[string]string{"status": "updated"})
}

func (a *App) handleDeleteKey(w http.ResponseWriter, r *http.Request) {
	if err := a.Store.DeleteKey(r.Context(), r.PathValue("id")); err == store.ErrNotFound {
		writeError(w, http.StatusNotFound, "key not found")
		return
	} else if err != nil {
		writeInternalError(w, r, "delete key failed", err)
		return
	}
	w.WriteHeader(http.StatusNoContent)
}

func (a *App) handleSetKeyEnabled(enabled bool) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		id := r.PathValue("id")

		// Only re-enabling needs the doc_url check: a disabled key isn't
		// running a worker and can't collide with anything (see
		// HasEnabledKeyWithDocURL's doc comment), and disabling never
		// changes doc_url so it can't create a collision either.
		if enabled {
			k, err := a.Store.GetKeyByID(r.Context(), id)
			if err == store.ErrNotFound {
				writeError(w, http.StatusNotFound, "key not found")
				return
			} else if err != nil {
				writeInternalError(w, r, "get key failed", err)
				return
			}
			inUse, err := a.Store.HasEnabledKeyWithDocURL(r.Context(), k.DocURL, id)
			if err != nil {
				writeInternalError(w, r, "check doc_url failed", err)
				return
			}
			if inUse {
				writeError(w, http.StatusConflict, "doc_url is already used by another enabled key - each key needs its own Yandex Docs document")
				return
			}
		}

		if err := a.Store.SetKeyEnabled(r.Context(), id, enabled); err == store.ErrNotFound {
			writeError(w, http.StatusNotFound, "key not found")
			return
		} else if err != nil {
			writeInternalError(w, r, "update key failed", err)
			return
		}
		writeJSON(w, http.StatusOK, map[string]bool{"enabled": enabled})
	}
}
