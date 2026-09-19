package store

import (
	"context"
	"fmt"

	"openflux-control/internal/model"
)

// ApplyUsageDeltas batches into 2-3 statements total via UNNEST'd arrays instead of one round trip per key, since 1000+ active keys reporting every interval used to hold row locks for however long that took.
func (s *Store) ApplyUsageDeltas(ctx context.Context, deltas []model.UsageDelta) ([]string, error) {
	if len(deltas) == 0 {
		return nil, nil
	}

	ids := make([]string, len(deltas))
	sent := make([]int64, len(deltas))
	recv := make([]int64, len(deltas))
	for i, d := range deltas {
		ids[i] = d.KeyID
		sent[i] = d.BytesSentDelta
		recv[i] = d.BytesReceivedDelta
	}

	tx, err := s.pool.Begin(ctx)
	if err != nil {
		return nil, fmt.Errorf("begin usage tx: %w", err)
	}
	defer tx.Rollback(ctx)

	rows, err := tx.Query(ctx, `
		UPDATE keys k
		SET bytes_sent_total = k.bytes_sent_total + d.sent,
		    bytes_received_total = k.bytes_received_total + d.recv,
		    updated_at = now(),
		    last_seen_at = now()
		FROM (
			SELECT UNNEST($1::text[])::uuid AS id, UNNEST($2::bigint[]) AS sent, UNNEST($3::bigint[]) AS recv
		) d
		WHERE k.id = d.id
		RETURNING k.id, k.bytes_sent_total, k.bytes_received_total, k.traffic_limit_bytes, k.enabled
	`, ids, sent, recv)
	if err != nil {
		return nil, fmt.Errorf("apply usage deltas: %w", err)
	}

	var overQuota []string
	for rows.Next() {
		var id string
		var bytesSent, bytesReceived int64
		var limit *int64
		var enabled bool
		if err := rows.Scan(&id, &bytesSent, &bytesReceived, &limit, &enabled); err != nil {
			rows.Close()
			return nil, fmt.Errorf("scan usage update: %w", err)
		}
		if enabled && limit != nil && bytesSent+bytesReceived >= *limit {
			overQuota = append(overQuota, id)
		}
	}
	// rows.Next() returning false already closed rows (pgx does this internally); a following statement on the same tx would otherwise fail as "conn busy".
	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("iterate usage update: %w", err)
	}

	if _, err := tx.Exec(ctx, `
		INSERT INTO usage_daily (key_id, day, bytes_sent, bytes_received)
		SELECT UNNEST($1::text[])::uuid, CURRENT_DATE, UNNEST($2::bigint[]), UNNEST($3::bigint[])
		ON CONFLICT (key_id, day) DO UPDATE
		SET bytes_sent = usage_daily.bytes_sent + excluded.bytes_sent,
		    bytes_received = usage_daily.bytes_received + excluded.bytes_received
	`, ids, sent, recv); err != nil {
		return nil, fmt.Errorf("upsert usage_daily: %w", err)
	}

	var disabledNow []string
	if len(overQuota) > 0 {
		tag, err := tx.Exec(ctx, `UPDATE keys SET enabled = false, updated_at = now() WHERE id::text = ANY($1::text[])`, overQuota)
		if err != nil {
			return nil, fmt.Errorf("auto-disable over-quota keys: %w", err)
		}
		if tag.RowsAffected() > 0 {
			disabledNow = overQuota
		}
	}

	if err := tx.Commit(ctx); err != nil {
		return nil, fmt.Errorf("commit usage tx: %w", err)
	}

	return disabledNow, nil
}

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
