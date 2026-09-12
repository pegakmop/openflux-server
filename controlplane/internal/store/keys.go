package store

import (
	"context"
	"errors"
	"fmt"
	"time"

	"github.com/jackc/pgx/v5"

	"openflux-control/internal/model"
)

type CreateKeyParams struct {
	TokenHash         string
	Label             string
	Transport         string
	DocURL            string
	TrafficLimitBytes *int64
	OwnerRef          string
	ExpiresAt         *time.Time
}

func scanKey(row pgx.Row) (model.Key, error) {
	var k model.Key
	err := row.Scan(
		&k.ID, &k.Label, &k.Transport, &k.DocURL, &k.AssignedNodeID, &k.Enabled,
		&k.TrafficLimitBytes, &k.BytesSentTotal, &k.BytesReceivedTotal, &k.OwnerRef,
		&k.ExpiresAt, &k.CreatedAt, &k.UpdatedAt, &k.LastSeenAt,
	)
	return k, err
}

const keyColumns = `id, label, transport, doc_url, assigned_node_id, enabled,
	traffic_limit_bytes, bytes_sent_total, bytes_received_total, owner_ref,
	expires_at, created_at, updated_at, last_seen_at`

// CreateKey inserts a new key and best-effort assigns it to the active node
// currently carrying the fewest enabled keys (so a fleet of exit nodes stays
// roughly balanced without needing a separate scheduler). If no node has
// spare capacity the key is left unassigned; an operator can assign one
// later via UpdateKeyAssignedNode.
func (s *Store) CreateKey(ctx context.Context, p CreateKeyParams) (model.Key, error) {
	row := s.pool.QueryRow(ctx, `
		WITH candidate AS (
			SELECT n.id
			FROM nodes n
			LEFT JOIN keys k ON k.assigned_node_id = n.id AND k.enabled = true
			WHERE n.status = 'active'
			GROUP BY n.id
			HAVING count(k.id) < n.max_keys
			ORDER BY count(k.id) ASC
			LIMIT 1
		)
		INSERT INTO keys (token_hash, label, transport, doc_url, traffic_limit_bytes, owner_ref, expires_at, assigned_node_id)
		VALUES ($1, $2, $3, $4, $5, $6, $7, (SELECT id FROM candidate))
		RETURNING `+keyColumns,
		p.TokenHash, p.Label, p.Transport, p.DocURL, p.TrafficLimitBytes, p.OwnerRef, p.ExpiresAt)

	return scanKey(row)
}

func (s *Store) GetKeyByTokenHash(ctx context.Context, tokenHash string) (model.Key, error) {
	row := s.pool.QueryRow(ctx, `SELECT `+keyColumns+` FROM keys WHERE token_hash = $1`, tokenHash)
	k, err := scanKey(row)
	if errors.Is(err, pgx.ErrNoRows) {
		return model.Key{}, ErrNotFound
	}
	if err != nil {
		return model.Key{}, fmt.Errorf("get key by token: %w", err)
	}
	return k, nil
}

func (s *Store) GetKeyByID(ctx context.Context, id string) (model.Key, error) {
	row := s.pool.QueryRow(ctx, `SELECT `+keyColumns+` FROM keys WHERE id = $1`, id)
	k, err := scanKey(row)
	if errors.Is(err, pgx.ErrNoRows) {
		return model.Key{}, ErrNotFound
	}
	if err != nil {
		return model.Key{}, fmt.Errorf("get key: %w", err)
	}
	return k, nil
}

// ListActiveKeysForNode returns the enabled keys currently assigned to a
// node - this is what the exit node's poll loop consumes.
func (s *Store) ListActiveKeysForNode(ctx context.Context, nodeID string) ([]model.Key, error) {
	rows, err := s.pool.Query(ctx, `
		SELECT `+keyColumns+` FROM keys
		WHERE assigned_node_id = $1 AND enabled = true
		ORDER BY created_at
	`, nodeID)
	if err != nil {
		return nil, fmt.Errorf("list node keys: %w", err)
	}
	defer rows.Close()

	var out []model.Key
	for rows.Next() {
		k, err := scanKey(rows)
		if err != nil {
			return nil, fmt.Errorf("scan node key: %w", err)
		}
		out = append(out, k)
	}
	return out, rows.Err()
}

type ListKeysFilter struct {
	OwnerRef string
	Limit    int
	Offset   int
}

func (s *Store) ListKeys(ctx context.Context, f ListKeysFilter) ([]model.Key, error) {
	limit := f.Limit
	if limit <= 0 || limit > 500 {
		limit = 100
	}

	rows, err := s.pool.Query(ctx, `
		SELECT `+keyColumns+` FROM keys
		WHERE ($1 = '' OR owner_ref = $1)
		ORDER BY created_at DESC
		LIMIT $2 OFFSET $3
	`, f.OwnerRef, limit, f.Offset)
	if err != nil {
		return nil, fmt.Errorf("list keys: %w", err)
	}
	defer rows.Close()

	var out []model.Key
	for rows.Next() {
		k, err := scanKey(rows)
		if err != nil {
			return nil, fmt.Errorf("scan key: %w", err)
		}
		out = append(out, k)
	}
	return out, rows.Err()
}

// HasEnabledKeyWithDocURL reports whether some other enabled key already
// uses docURL. Yandex broadcasts every "cursor"/"saveChanges" event in a
// document to every participant, sender included - two keys sharing one
// document means two independent tunnels sit in the same broadcast room and
// each one's traffic gets reinjected into the other's, corrupting both.
// excludeID skips a key checking against itself (e.g. re-enabling itself
// with an unchanged doc_url).
func (s *Store) HasEnabledKeyWithDocURL(ctx context.Context, docURL, excludeID string) (bool, error) {
	var exists bool
	err := s.pool.QueryRow(ctx, `
		SELECT EXISTS(SELECT 1 FROM keys WHERE doc_url = $1 AND enabled = true AND id != $2)
	`, docURL, excludeID).Scan(&exists)
	if err != nil {
		return false, fmt.Errorf("check doc_url in use: %w", err)
	}
	return exists, nil
}

func (s *Store) SetKeyEnabled(ctx context.Context, id string, enabled bool) error {
	tag, err := s.pool.Exec(ctx, `UPDATE keys SET enabled = $1, updated_at = now() WHERE id = $2`, enabled, id)
	if err != nil {
		return fmt.Errorf("set key enabled: %w", err)
	}
	if tag.RowsAffected() == 0 {
		return ErrNotFound
	}
	return nil
}

// RotateKeyToken invalidates a key's current token and issues a new one,
// leaving every other field (label, doc_url, traffic limit, owner_ref,
// usage stats) untouched - for when the raw token from creation is gone
// (it's only ever stored hashed) but the key itself should keep working.
func (s *Store) RotateKeyToken(ctx context.Context, id, newTokenHash string) (model.Key, error) {
	row := s.pool.QueryRow(ctx, `
		UPDATE keys SET token_hash = $1, updated_at = now() WHERE id = $2
		RETURNING `+keyColumns, newTokenHash, id)
	k, err := scanKey(row)
	if errors.Is(err, pgx.ErrNoRows) {
		return model.Key{}, ErrNotFound
	}
	if err != nil {
		return model.Key{}, fmt.Errorf("rotate key token: %w", err)
	}
	return k, nil
}

func (s *Store) SetKeyTrafficLimit(ctx context.Context, id string, limit *int64) error {
	tag, err := s.pool.Exec(ctx, `UPDATE keys SET traffic_limit_bytes = $1, updated_at = now() WHERE id = $2`, limit, id)
	if err != nil {
		return fmt.Errorf("set key traffic limit: %w", err)
	}
	if tag.RowsAffected() == 0 {
		return ErrNotFound
	}
	return nil
}

func (s *Store) DeleteKey(ctx context.Context, id string) error {
	tag, err := s.pool.Exec(ctx, `DELETE FROM keys WHERE id = $1`, id)
	if err != nil {
		return fmt.Errorf("delete key: %w", err)
	}
	if tag.RowsAffected() == 0 {
		return ErrNotFound
	}
	return nil
}
