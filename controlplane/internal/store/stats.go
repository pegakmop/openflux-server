package store

import (
	"context"
	"fmt"
)

type StatsSummary struct {
	TotalBytesSent     int64
	TotalBytesReceived int64
	TodayBytesSent     int64
	TodayBytesReceived int64
	TotalKeys          int
	EnabledKeys        int
	OverQuotaKeys      int
	ExpiredKeys        int
	TotalNodes         int
	OnlineNodes        int
}

// StatsSummary computes the whole-fleet numbers the dashboard renders as
// its stat cards, in a handful of aggregate queries - deliberately not
// derived client-side from the (bounded at 100 rows) key list.
func (s *Store) StatsSummary(ctx context.Context) (StatsSummary, error) {
	var out StatsSummary

	err := s.pool.QueryRow(ctx, `
		SELECT
			coalesce(sum(bytes_sent_total), 0),
			coalesce(sum(bytes_received_total), 0),
			count(*),
			count(*) FILTER (WHERE enabled),
			count(*) FILTER (WHERE enabled AND traffic_limit_bytes IS NOT NULL
			                AND bytes_sent_total + bytes_received_total >= traffic_limit_bytes),
			count(*) FILTER (WHERE enabled AND expires_at IS NOT NULL AND expires_at < now())
		FROM keys
	`).Scan(&out.TotalBytesSent, &out.TotalBytesReceived, &out.TotalKeys, &out.EnabledKeys, &out.OverQuotaKeys, &out.ExpiredKeys)
	if err != nil {
		return StatsSummary{}, fmt.Errorf("summary keys: %w", err)
	}

	err = s.pool.QueryRow(ctx, `
		SELECT coalesce(sum(bytes_sent), 0), coalesce(sum(bytes_received), 0)
		FROM usage_daily WHERE day = CURRENT_DATE
	`).Scan(&out.TodayBytesSent, &out.TodayBytesReceived)
	if err != nil {
		return StatsSummary{}, fmt.Errorf("summary today: %w", err)
	}

	err = s.pool.QueryRow(ctx, `
		SELECT
			count(*),
			count(*) FILTER (WHERE last_heartbeat_at IS NOT NULL AND last_heartbeat_at > now() - interval '3 minutes')
		FROM nodes
	`).Scan(&out.TotalNodes, &out.OnlineNodes)
	if err != nil {
		return StatsSummary{}, fmt.Errorf("summary nodes: %w", err)
	}

	return out, nil
}
