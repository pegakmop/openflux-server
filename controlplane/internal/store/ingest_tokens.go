package store

import (
	"context"
	"errors"
	"fmt"

	"github.com/jackc/pgx/v5"

	"openflux-control/internal/model"
)

func (s *Store) CreateIngestToken(ctx context.Context, tokenHash, label, scope string) (model.IngestToken, error) {
	var t model.IngestToken
	err := s.pool.QueryRow(ctx, `
		INSERT INTO ingest_tokens (token_hash, label, scope)
		VALUES ($1, $2, $3)
		RETURNING id, label, scope, enabled, created_at
	`, tokenHash, label, scope).Scan(&t.ID, &t.Label, &t.Scope, &t.Enabled, &t.CreatedAt)
	if err != nil {
		return model.IngestToken{}, fmt.Errorf("insert ingest token: %w", err)
	}
	return t, nil
}

func (s *Store) GetIngestTokenByHash(ctx context.Context, tokenHash string) (model.IngestToken, error) {
	var t model.IngestToken
	err := s.pool.QueryRow(ctx, `
		SELECT id, label, scope, enabled, created_at
		FROM ingest_tokens WHERE token_hash = $1 AND enabled = true
	`, tokenHash).Scan(&t.ID, &t.Label, &t.Scope, &t.Enabled, &t.CreatedAt)
	if errors.Is(err, pgx.ErrNoRows) {
		return model.IngestToken{}, ErrNotFound
	}
	if err != nil {
		return model.IngestToken{}, fmt.Errorf("get ingest token: %w", err)
	}
	return t, nil
}

// ListIngestTokens returns every ingest token (enabled or not) so an admin
// can review what's been issued and revoke one that's leaked or retired -
// nothing else in this package could previously do that.
func (s *Store) ListIngestTokens(ctx context.Context) ([]model.IngestToken, error) {
	rows, err := s.pool.Query(ctx, `
		SELECT id, label, scope, enabled, created_at
		FROM ingest_tokens ORDER BY created_at DESC
	`)
	if err != nil {
		return nil, fmt.Errorf("list ingest tokens: %w", err)
	}
	defer rows.Close()

	var out []model.IngestToken
	for rows.Next() {
		var t model.IngestToken
		if err := rows.Scan(&t.ID, &t.Label, &t.Scope, &t.Enabled, &t.CreatedAt); err != nil {
			return nil, fmt.Errorf("scan ingest token: %w", err)
		}
		out = append(out, t)
	}
	return out, rows.Err()
}

func (s *Store) SetIngestTokenEnabled(ctx context.Context, id string, enabled bool) error {
	tag, err := s.pool.Exec(ctx, `UPDATE ingest_tokens SET enabled = $1 WHERE id = $2`, enabled, id)
	if err != nil {
		return fmt.Errorf("set ingest token enabled: %w", err)
	}
	if tag.RowsAffected() == 0 {
		return ErrNotFound
	}
	return nil
}
