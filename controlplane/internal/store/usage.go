package store

import (
	"context"
	"fmt"

	"openflux-control/internal/model"
)

// ApplyUsageDeltas atomically adds the reported byte deltas to each key's
// running totals and today's usage_daily rollup, then auto-disables any key
// that just crossed its traffic_limit_bytes. It returns the IDs of keys
// disabled by this call so the caller (the node-facing handler) can tell the
// reporting node to tear those workers down immediately instead of waiting
// for its next poll.
func (s *Store) ApplyUsageDeltas(ctx context.Context, deltas []model.UsageDelta) ([]string, error) {
	if len(deltas) == 0 {
		return nil, nil
	}

	tx, err := s.pool.Begin(ctx)
	if err != nil {
		return nil, fmt.Errorf("begin usage tx: %w", err)
	}
	defer tx.Rollback(ctx)

	var disabledNow []string

	for _, d := range deltas {
		var bytesSent, bytesReceived int64
		var limit *int64
		var enabled bool

		err := tx.QueryRow(ctx, `
			UPDATE keys
			SET bytes_sent_total = bytes_sent_total + $1,
			    bytes_received_total = bytes_received_total + $2,
			    updated_at = now(),
			    last_seen_at = now()
			WHERE id = $3
			RETURNING bytes_sent_total, bytes_received_total, traffic_limit_bytes, enabled
		`, d.BytesSentDelta, d.BytesReceivedDelta, d.KeyID).Scan(&bytesSent, &bytesReceived, &limit, &enabled)
		if err != nil {
			return nil, fmt.Errorf("apply usage delta for key %s: %w", d.KeyID, err)
		}

		if _, err := tx.Exec(ctx, `
			INSERT INTO usage_daily (key_id, day, bytes_sent, bytes_received)
			VALUES ($1, CURRENT_DATE, $2, $3)
			ON CONFLICT (key_id, day) DO UPDATE
			SET bytes_sent = usage_daily.bytes_sent + excluded.bytes_sent,
			    bytes_received = usage_daily.bytes_received + excluded.bytes_received
		`, d.KeyID, d.BytesSentDelta, d.BytesReceivedDelta); err != nil {
			return nil, fmt.Errorf("upsert usage_daily for key %s: %w", d.KeyID, err)
		}

		if enabled && limit != nil && bytesSent+bytesReceived >= *limit {
			if _, err := tx.Exec(ctx, `UPDATE keys SET enabled = false, updated_at = now() WHERE id = $1`, d.KeyID); err != nil {
				return nil, fmt.Errorf("auto-disable key %s: %w", d.KeyID, err)
			}
			disabledNow = append(disabledNow, d.KeyID)
		}
	}

	if err := tx.Commit(ctx); err != nil {
		return nil, fmt.Errorf("commit usage tx: %w", err)
	}

	return disabledNow, nil
}

// AggregateUsageDays returns per-day traffic totals across every key for the
// last days days (the current day included). Empty days are simply absent
// from the result - the caller can decide how to render gaps.
func (s *Store) AggregateUsageDays(ctx context.Context, days int) ([]model.UsageDay, error) {
	if days <= 0 {
		days = 30
	}
	if days > 365 {
		days = 365
	}

	rows, err := s.pool.Query(ctx, `
		SELECT day, sum(bytes_sent)::bigint, sum(bytes_received)::bigint, count(DISTINCT key_id)::int
		FROM usage_daily
		WHERE day >= CURRENT_DATE - ($1::int - 1)
		GROUP BY day
		ORDER BY day ASC
	`, days)
	if err != nil {
		return nil, fmt.Errorf("aggregate usage days: %w", err)
	}
	defer rows.Close()

	return scanUsageDays(rows)
}

// KeyUsageDays returns the per-day traffic totals for a single key over the
// last days days. Deleting a key cascades its usage_daily rows away, so a
// deleted key simply returns an empty list.
func (s *Store) KeyUsageDays(ctx context.Context, keyID string, days int) ([]model.UsageDay, error) {
	if days <= 0 {
		days = 30
	}
	if days > 365 {
		days = 365
	}

	rows, err := s.pool.Query(ctx, `
		SELECT day, bytes_sent, bytes_received, 1::int AS active_keys
		FROM usage_daily
		WHERE key_id = $1 AND day >= CURRENT_DATE - ($2::int - 1)
		ORDER BY day ASC
	`, keyID, days)
	if err != nil {
		return nil, fmt.Errorf("key usage days: %w", err)
	}
	defer rows.Close()

	return scanUsageDays(rows)
}

type usageRowScanner interface {
	Next() bool
	Scan(dest ...any) error
	Err() error
}

func scanUsageDays(rows usageRowScanner) ([]model.UsageDay, error) {
	var out []model.UsageDay
	for rows.Next() {
		var u model.UsageDay
		if err := rows.Scan(&u.Day, &u.BytesSent, &u.BytesReceived, &u.ActiveKeyCount); err != nil {
			return nil, fmt.Errorf("scan usage day: %w", err)
		}
		out = append(out, u)
	}
	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("iterate usage days: %w", err)
	}
	return out, nil
}
