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

func (s *Store) StatsSummary(ctx context.Context) (StatsSummary, error) {
	var out StatsSummary

	// bytes_sent_total/bytes_received_total are summed across every key including deleted ones, matching DeleteKey's soft-delete lifetime totals; other counts exclude deleted keys.
	err := s.pool.QueryRow(ctx, `
		SELECT
			coalesce(sum(bytes_sent_total), 0),
			coalesce(sum(bytes_received_total), 0),
			count(*) FILTER (WHERE deleted_at IS NULL),
			count(*) FILTER (WHERE deleted_at IS NULL AND enabled),
			count(*) FILTER (WHERE deleted_at IS NULL AND enabled AND traffic_limit_bytes IS NOT NULL
			                AND bytes_sent_total + bytes_received_total >= traffic_limit_bytes),
			count(*) FILTER (WHERE deleted_at IS NULL AND enabled AND expires_at IS NOT NULL AND expires_at < now())
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
