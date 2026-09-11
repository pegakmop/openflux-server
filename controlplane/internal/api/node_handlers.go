package api

import (
	"net/http"

	"openflux-control/internal/model"
	"openflux-control/internal/store"
)

type nodeKeyResponse struct {
	ID                string `json:"id"`
	DocURL            string `json:"doc_url"`
	Transport         string `json:"transport"`
	TrafficLimitBytes *int64 `json:"traffic_limit_bytes,omitempty"`
	BytesUsedTotal    int64  `json:"bytes_used_total"`
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
		out = append(out, nodeKeyResponse{
			ID:                k.ID,
			DocURL:            k.DocURL,
			Transport:         k.Transport,
			TrafficLimitBytes: k.TrafficLimitBytes,
			BytesUsedTotal:    k.BytesUsedTotal(),
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
