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
	TokenHash string
	TokenEnc  []byte
	Label     string
	Transport string
	DocURL    string
	// DocURLs is set instead of DocURL for the yandex_multistream transport
	// (2+ URLs) - see model.Key.DocURLs.
	DocURLs           []string
	TrafficLimitBytes *int64
	OwnerRef          string
	ExpiresAt         *time.Time
}

func scanKey(row pgx.Row) (model.Key, error) {
	var k model.Key
	err := row.Scan(
		&k.ID, &k.Label, &k.Transport, &k.DocURL, &k.DocURLs, &k.AssignedNodeID, &k.Enabled,
		&k.TrafficLimitBytes, &k.BytesSentTotal, &k.BytesReceivedTotal, &k.OwnerRef,
		&k.ExpiresAt, &k.CreatedAt, &k.UpdatedAt, &k.LastSeenAt,
	)
	return k, err
}

const keyColumns = `id, label, transport, doc_url, doc_urls, assigned_node_id, enabled,
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
		INSERT INTO keys (token_hash, token_enc, label, transport, doc_url, doc_urls, traffic_limit_bytes, owner_ref, expires_at, assigned_node_id)
		VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, (SELECT id FROM candidate))
		RETURNING `+keyColumns,
		p.TokenHash, p.TokenEnc, p.Label, p.Transport, p.DocURL, p.DocURLs, p.TrafficLimitBytes, p.OwnerRef, p.ExpiresAt)

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

// NodeKey is a key as its assigned exit node sees it - a narrower shape
// than model.Key because it includes TokenEnc, the encrypted raw token
// (see auth.TokenCipher) a node needs to derive the same end-to-end
// encryption key the client did. Keeping this separate from model.Key
// means nothing else that touches the general key model (the admin API,
// the web panel) has any path to that field at all.
type NodeKey struct {
	ID                 string
	DocURL             string
	DocURLs            []string
	Transport          string
	TrafficLimitBytes  *int64
	BytesSentTotal     int64
	BytesReceivedTotal int64
	TokenEnc           []byte
}

// ListActiveKeysForNode returns the enabled keys currently assigned to a
// node - this is what the exit node's poll loop consumes.
func (s *Store) ListActiveKeysForNode(ctx context.Context, nodeID string) ([]NodeKey, error) {
	rows, err := s.pool.Query(ctx, `
		SELECT id, doc_url, doc_urls, transport, traffic_limit_bytes, bytes_sent_total, bytes_received_total, token_enc
		FROM keys
		WHERE assigned_node_id = $1 AND enabled = true
		ORDER BY created_at
	`, nodeID)
	if err != nil {
		return nil, fmt.Errorf("list node keys: %w", err)
	}
	defer rows.Close()

	var out []NodeKey
	for rows.Next() {
		var k NodeKey
		if err := rows.Scan(&k.ID, &k.DocURL, &k.DocURLs, &k.Transport, &k.TrafficLimitBytes,
			&k.BytesSentTotal, &k.BytesReceivedTotal, &k.TokenEnc); err != nil {
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

// HasEnabledKeyWithAnyDocURL reports whether some OTHER enabled key already
// uses any of urls - checked against both doc_url (every non-multistream
// transport) and doc_urls (yandex_multistream's 2+ URLs), since both
// represent the same kind of collision. Yandex broadcasts every
// "cursor"/"saveChanges" event in a document to every participant, sender
// included - two keys sharing one document means two independent tunnels
// (or two legs of the same multistream key and someone else's key) sit in
// the same broadcast room and each one's traffic gets reinjected into the
// other's, corrupting both. excludeID skips a key checking against itself
// (e.g. re-enabling itself with unchanged URLs).
func (s *Store) HasEnabledKeyWithAnyDocURL(ctx context.Context, urls []string, excludeID string) (bool, error) {
	var exists bool
	// excludeID is "" on creation (there's no id yet) - id is uuid, and
	// casting "" to uuid errors outright ("invalid input syntax for type
	// uuid"), which used to surface here as "check doc_url failed" on every
	// single key creation. $2 = '' short-circuits before the ::uuid cast
	// ever runs, the same idiom ListKeys already uses above for its own
	// optional owner_ref filter.
	err := s.pool.QueryRow(ctx, `
		SELECT EXISTS(
			SELECT 1 FROM keys
			WHERE enabled = true
				AND ($2 = '' OR id != $2::uuid)
				AND (doc_url = ANY($1::text[]) OR doc_urls && $1::text[])
		)
	`, urls, excludeID).Scan(&exists)
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
func (s *Store) RotateKeyToken(ctx context.Context, id, newTokenHash string, newTokenEnc []byte) (model.Key, error) {
	row := s.pool.QueryRow(ctx, `
		UPDATE keys SET token_hash = $1, token_enc = $2, updated_at = now() WHERE id = $3
		RETURNING `+keyColumns, newTokenHash, newTokenEnc, id)
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
