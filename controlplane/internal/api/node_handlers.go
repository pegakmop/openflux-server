package api

import (
	"net/http"

	"openflux-control/internal/model"
	"openflux-control/internal/store"
)

type nodeKeyResponse struct {
	ID                string   `json:"id"`
	DocURL            string   `json:"doc_url"`
	DocURLs           []string `json:"doc_urls,omitempty"`
	Transport         string   `json:"transport"`
	TrafficLimitBytes *int64   `json:"traffic_limit_bytes,omitempty"`
	BytesUsedTotal    int64    `json:"bytes_used_total"`
	// Token is the raw key token, decrypted here (only a node-authenticated
	// request reaches this handler at all) so the assigned worker can derive
	// the same end-to-end encryption key the client used - see
	// transport.NewEncryptedTransport. Empty for a key created before this
	// existed (token_enc is null).
	Token string `json:"token,omitempty"`
	// E2EEncryption is the operator's binding decision for this key (see
	// model.Key.E2EEncryption) - the node wraps the worker's transport in
	// transport.EncryptedTransport if and only if this is true, matching
	// exactly what the client does with the same flag from its own deep
	// link. Previously the node had no way to see this setting at all and
	// guessed per-peer instead, making the panel's toggle purely advisory.
	E2EEncryption bool `json:"e2e_encryption,omitempty"`
}

func (a *App) handleNodeListKeys(w http.ResponseWriter, r *http.Request) {
	n := nodeFromContext(r.Context())

	keys, err := a.Store.ListActiveKeysForNode(r.Context(), n.ID)
	if err != nil {
		writeInternalError(w, r, "list keys failed", err)
		return
	}

	out := make([]nodeKeyResponse, 0, len(keys))
	for _, k := range keys {
		var token string
		if len(k.TokenEnc) > 0 {
			if t, err := a.Cipher.Decrypt(k.TokenEnc); err == nil {
				token = t
			}
		}
		out = append(out, nodeKeyResponse{
			ID:                k.ID,
			DocURL:            k.DocURL,
			DocURLs:           k.DocURLs,
			Transport:         k.Transport,
			TrafficLimitBytes: k.TrafficLimitBytes,
			BytesUsedTotal:    k.BytesSentTotal + k.BytesReceivedTotal,
			Token:             token,
			E2EEncryption:     k.E2EEncryption,
		})
	}
	writeJSON(w, http.StatusOK, out)
}

type usageDeltaRequest struct {
	KeyID              string `json:"key_id"`
	BytesSentDelta     int64  `json:"bytes_sent_delta"`
	BytesReceivedDelta int64  `json:"bytes_received_delta"`
}

type reportUsageRequest struct {
	Deltas []usageDeltaRequest `json:"deltas"`
}

func (a *App) handleNodeUsage(w http.ResponseWriter, r *http.Request) {
	var req reportUsageRequest
	if err := readJSON(r, &req); err != nil {
		writeError(w, http.StatusBadRequest, "invalid body")
		return
	}
	if len(req.Deltas) > 1000 {
		writeError(w, http.StatusBadRequest, "too many deltas in one report (max 1000)")
		return
	}

	deltas := make([]model.UsageDelta, 0, len(req.Deltas))
	for _, d := range req.Deltas {
		if d.KeyID == "" {
			continue
		}
		deltas = append(deltas, model.UsageDelta{
			KeyID:              d.KeyID,
			BytesSentDelta:     d.BytesSentDelta,
			BytesReceivedDelta: d.BytesReceivedDelta,
		})
	}

	disabledNow, err := a.Store.ApplyUsageDeltas(r.Context(), deltas)
	if err != nil {
		writeInternalError(w, r, "apply usage failed", err)
		return
	}

	writeJSON(w, http.StatusOK, map[string][]string{"disabled_now": disabledNow})
}

func (a *App) handleNodeHeartbeat(w http.ResponseWriter, r *http.Request) {
	n := nodeFromContext(r.Context())

	if err := a.Store.TouchNodeHeartbeat(r.Context(), n.ID); err == store.ErrNotFound {
		writeError(w, http.StatusNotFound, "node not found")
		return
	} else if err != nil {
		writeInternalError(w, r, "heartbeat failed", err)
		return
	}

	writeJSON(w, http.StatusOK, map[string]string{"status": "ok"})
}
