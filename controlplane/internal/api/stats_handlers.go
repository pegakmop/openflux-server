package api

import (
	"net/http"
	"strconv"
)

type usageDayResponse struct {
	Day            string `json:"day"`
	BytesSent      int64  `json:"bytes_sent"`
	BytesReceived  int64  `json:"bytes_received"`
	ActiveKeyCount int    `json:"active_keys"`
}

func toUsageDayResponse(day string, bytesSent, bytesReceived int64, activeKeys int) usageDayResponse {
	return usageDayResponse{Day: day, BytesSent: bytesSent, BytesReceived: bytesReceived, ActiveKeyCount: activeKeys}
}

// clampDays bounds the ?days= query param. Returns 30 when absent or bogus.
func clampDays(v string) int {
	if v == "" {
		return 30
	}
	n, err := strconv.Atoi(v)
	if err != nil || n < 1 {
		return 30
	}
	if n > 365 {
		return 365
	}
	return n
}

// handleStatsSummary backs the dashboard's stat cards.
func (a *App) handleStatsSummary(w http.ResponseWriter, r *http.Request) {
	sum, err := a.Store.StatsSummary(r.Context())
	if err != nil {
		writeInternalError(w, r, "stats summary failed", err)
		return
	}
	writeJSON(w, http.StatusOK, map[string]interface{}{
		"total_bytes_sent":     sum.TotalBytesSent,
		"total_bytes_received": sum.TotalBytesReceived,
		"today_bytes_sent":     sum.TodayBytesSent,
		"today_bytes_received": sum.TodayBytesReceived,
		"total_keys":           sum.TotalKeys,
		"enabled_keys":         sum.EnabledKeys,
		"over_quota_keys":      sum.OverQuotaKeys,
		"expired_keys":         sum.ExpiredKeys,
		"total_nodes":          sum.TotalNodes,
		"online_nodes":         sum.OnlineNodes,
	})
}

// handleStatsUsage returns per-day aggregate traffic across all keys.
func (a *App) handleStatsUsage(w http.ResponseWriter, r *http.Request) {
	days := clampDays(r.URL.Query().Get("days"))
	rows, err := a.Store.AggregateUsageDays(r.Context(), days)
	if err != nil {
		writeInternalError(w, r, "usage stats failed", err)
		return
	}
	out := make([]usageDayResponse, 0, len(rows))
	for _, u := range rows {
		out = append(out, toUsageDayResponse(u.Day, u.BytesSent, u.BytesReceived, u.ActiveKeyCount))
	}
	writeJSON(w, http.StatusOK, out)
}

// handleKeyUsage returns per-day traffic for one key (empty for a deleted key).
func (a *App) handleKeyUsage(w http.ResponseWriter, r *http.Request) {
	days := clampDays(r.URL.Query().Get("days"))
	rows, err := a.Store.KeyUsageDays(r.Context(), r.PathValue("id"), days)
	if err != nil {
		writeInternalError(w, r, "key usage stats failed", err)
		return
	}
	out := make([]usageDayResponse, 0, len(rows))
	for _, u := range rows {
		out = append(out, toUsageDayResponse(u.Day, u.BytesSent, u.BytesReceived, u.ActiveKeyCount))
	}
	writeJSON(w, http.StatusOK, out)
}
