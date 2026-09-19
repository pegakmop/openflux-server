package store

import (
	"context"
	"errors"
	"fmt"
	"hash/fnv"
	"sort"
	"time"

	"github.com/jackc/pgx/v5"

	"openflux-control/internal/model"
)

type CreateKeyParams struct {
	TokenHash         string
	TokenEnc          []byte
	Label             string
	Transport         string
	DocURL            string
	DocURLs           []string
	E2EEncryption     bool
	TrafficLimitBytes *int64
	OwnerRef          string
	ExpiresAt         *time.Time
}

func scanKey(row pgx.Row) (model.Key, error) {
	var k model.Key
	err := row.Scan(
		&k.ID, &k.Label, &k.Transport, &k.DocURL, &k.DocURLs, &k.E2EEncryption, &k.AssignedNodeID, &k.Enabled,
		&k.TrafficLimitBytes, &k.BytesSentTotal, &k.BytesReceivedTotal, &k.OwnerRef,
		&k.ExpiresAt, &k.CreatedAt, &k.UpdatedAt, &k.LastSeenAt,
	)
	return k, err
}

const keyColumns = `id, label, transport, doc_url, doc_urls, e2e_encryption, assigned_node_id, enabled,
	traffic_limit_bytes, bytes_sent_total, bytes_received_total, owner_ref,
	expires_at, created_at, updated_at, last_seen_at`

// CreateKey best-effort assigns the new key to whichever active node currently carries the fewest enabled keys, keeping the fleet roughly balanced without a separate scheduler.
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
		INSERT INTO keys (token_hash, token_enc, label, transport, doc_url, doc_urls, e2e_encryption, traffic_limit_bytes, owner_ref, expires_at, assigned_node_id)
		VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10, (SELECT id FROM candidate))
		RETURNING `+keyColumns,
		p.TokenHash, p.TokenEnc, p.Label, p.Transport, p.DocURL, p.DocURLs, p.E2EEncryption, p.TrafficLimitBytes, p.OwnerRef, p.ExpiresAt)

	return scanKey(row)
}

func (s *Store) GetKeyByTokenHash(ctx context.Context, tokenHash string) (model.Key, error) {
	row := s.pool.QueryRow(ctx, `SELECT `+keyColumns+` FROM keys WHERE token_hash = $1 AND deleted_at IS NULL`, tokenHash)
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
	row := s.pool.QueryRow(ctx, `SELECT `+keyColumns+` FROM keys WHERE id = $1 AND deleted_at IS NULL`, id)
	k, err := scanKey(row)
	if errors.Is(err, pgx.ErrNoRows) {
		return model.Key{}, ErrNotFound
	}
	if err != nil {
		return model.Key{}, fmt.Errorf("get key: %w", err)
	}
	return k, nil
}

// NodeKey is kept separate from model.Key so nothing outside the exit-node path (the admin API, the web panel) has any access to TokenEnc, the encrypted raw token.
type NodeKey struct {
	ID                 string
	DocURL             string
	DocURLs            []string
	Transport          string
	TrafficLimitBytes  *int64
	BytesSentTotal     int64
	BytesReceivedTotal int64
	TokenEnc           []byte
	E2EEncryption      bool
}

func (s *Store) ListActiveKeysForNode(ctx context.Context, nodeID string) ([]NodeKey, error) {
	rows, err := s.pool.Query(ctx, `
		SELECT id, doc_url, doc_urls, transport, traffic_limit_bytes, bytes_sent_total, bytes_received_total, token_enc, e2e_encryption
		FROM keys
		WHERE assigned_node_id = $1 AND enabled = true AND deleted_at IS NULL
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
			&k.BytesSentTotal, &k.BytesReceivedTotal, &k.TokenEnc, &k.E2EEncryption); err != nil {
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
		WHERE ($1 = '' OR owner_ref = $1) AND deleted_at IS NULL
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

// HasEnabledKeyWithAnyDocURL guards against two enabled keys sharing one Yandex document: Yandex broadcasts every event to every participant, so a shared doc means each key's traffic gets reinjected into the other's.
func (s *Store) HasEnabledKeyWithAnyDocURL(ctx context.Context, urls []string, excludeID string) (bool, error) {
	var exists bool
	// excludeID is "" on creation; $2='' short-circuits before the ::uuid cast, which otherwise errors outright on an empty string ("invalid input syntax for type uuid").
	err := s.pool.QueryRow(ctx, `
		SELECT EXISTS(
			SELECT 1 FROM keys
			WHERE enabled = true
				AND deleted_at IS NULL
				AND ($2 = '' OR id != $2::uuid)
				AND (doc_url = ANY($1::text[]) OR doc_urls && $1::text[])
		)
	`, urls, excludeID).Scan(&exists)
	if err != nil {
		return false, fmt.Errorf("check doc_url in use: %w", err)
	}
	return exists, nil
}

// ErrDocURLInUse is returned when another enabled key already claims a doc URL, discovered inside the same transaction as the write so it can't lose a race against a concurrent check.
var ErrDocURLInUse = errors.New("doc_url already in use by another enabled key")

// docURLLockKeys sorts URLs so two calls with overlapping sets always acquire their shared locks in the same order and can never deadlock waiting on each other.
func docURLLockKeys(urls []string) []int64 {
	seen := make(map[int64]bool, len(urls))
	keys := make([]int64, 0, len(urls))
	for _, u := range urls {
		h := fnv.New64a()
		h.Write([]byte(u))
		k := int64(h.Sum64())
		if !seen[k] {
			seen[k] = true
			keys = append(keys, k)
		}
	}
	sort.Slice(keys, func(i, j int) bool { return keys[i] < keys[j] })
	return keys
}

// lockDocURLs closes the check-then-write race: two concurrent requests touching overlapping URL sets serialize here before either reaches its EXISTS check.
func lockDocURLs(ctx context.Context, tx pgx.Tx, urls []string) error {
	for _, key := range docURLLockKeys(urls) {
		if _, err := tx.Exec(ctx, `SELECT pg_advisory_xact_lock($1)`, key); err != nil {
			return fmt.Errorf("lock doc_url: %w", err)
		}
	}
	return nil
}

func docURLInUseTx(ctx context.Context, tx pgx.Tx, urls []string, excludeID string) (bool, error) {
	var exists bool
	err := tx.QueryRow(ctx, `
		SELECT EXISTS(
			SELECT 1 FROM keys
			WHERE enabled = true
				AND deleted_at IS NULL
				AND ($2 = '' OR id != $2::uuid)
				AND (doc_url = ANY($1::text[]) OR doc_urls && $1::text[])
		)
	`, urls, excludeID).Scan(&exists)
	if err != nil {
		return false, fmt.Errorf("check doc_url in use: %w", err)
	}
	return exists, nil
}

// CreateKeyGuarded holds an advisory lock across the doc_url check and insert in one transaction, since two concurrent creates could otherwise both pass the check before either committed.
func (s *Store) CreateKeyGuarded(ctx context.Context, p CreateKeyParams, urls []string) (model.Key, error) {
	tx, err := s.pool.Begin(ctx)
	if err != nil {
		return model.Key{}, fmt.Errorf("begin: %w", err)
	}
	defer tx.Rollback(ctx)

	if err := lockDocURLs(ctx, tx, urls); err != nil {
		return model.Key{}, err
	}

	inUse, err := docURLInUseTx(ctx, tx, urls, "")
	if err != nil {
		return model.Key{}, err
	}
	if inUse {
		return model.Key{}, ErrDocURLInUse
	}

	row := tx.QueryRow(ctx, `
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
		INSERT INTO keys (token_hash, token_enc, label, transport, doc_url, doc_urls, e2e_encryption, traffic_limit_bytes, owner_ref, expires_at, assigned_node_id)
		VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10, (SELECT id FROM candidate))
		RETURNING `+keyColumns,
		p.TokenHash, p.TokenEnc, p.Label, p.Transport, p.DocURL, p.DocURLs, p.E2EEncryption, p.TrafficLimitBytes, p.OwnerRef, p.ExpiresAt)
	k, err := scanKey(row)
	if err != nil {
		return model.Key{}, fmt.Errorf("create key: %w", err)
	}

	if err := tx.Commit(ctx); err != nil {
		return model.Key{}, fmt.Errorf("commit: %w", err)
	}
	return k, nil
}

func (s *Store) SetKeyEnabledGuarded(ctx context.Context, id string, urls []string) error {
	tx, err := s.pool.Begin(ctx)
	if err != nil {
		return fmt.Errorf("begin: %w", err)
	}
	defer tx.Rollback(ctx)

	if err := lockDocURLs(ctx, tx, urls); err != nil {
		return err
	}

	inUse, err := docURLInUseTx(ctx, tx, urls, id)
	if err != nil {
		return err
	}
	if inUse {
		return ErrDocURLInUse
	}

	tag, err := tx.Exec(ctx, `UPDATE keys SET enabled = true, updated_at = now() WHERE id = $1 AND deleted_at IS NULL`, id)
	if err != nil {
		return fmt.Errorf("set key enabled: %w", err)
	}
	if tag.RowsAffected() == 0 {
		return ErrNotFound
	}

	return tx.Commit(ctx)
}

func (s *Store) SetKeyEnabled(ctx context.Context, id string, enabled bool) error {
	tag, err := s.pool.Exec(ctx, `UPDATE keys SET enabled = $1, updated_at = now() WHERE id = $2 AND deleted_at IS NULL`, enabled, id)
	if err != nil {
		return fmt.Errorf("set key enabled: %w", err)
	}
	if tag.RowsAffected() == 0 {
		return ErrNotFound
	}
	return nil
}

func (s *Store) RotateKeyToken(ctx context.Context, id, newTokenHash string, newTokenEnc []byte) (model.Key, error) {
	row := s.pool.QueryRow(ctx, `
		UPDATE keys SET token_hash = $1, token_enc = $2, updated_at = now() WHERE id = $3 AND deleted_at IS NULL
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
	tag, err := s.pool.Exec(ctx, `UPDATE keys SET traffic_limit_bytes = $1, updated_at = now() WHERE id = $2 AND deleted_at IS NULL`, limit, id)
	if err != nil {
		return fmt.Errorf("set key traffic limit: %w", err)
	}
	if tag.RowsAffected() == 0 {
		return ErrNotFound
	}
	return nil
}

// DeleteKey soft-deletes: the row (and its lifetime usage totals) stays for the panel's charts, since every other Store method already treats deleted_at IS NOT NULL as "doesn't exist".
func (s *Store) DeleteKey(ctx context.Context, id string) error {
	tag, err := s.pool.Exec(ctx, `UPDATE keys SET deleted_at = now(), updated_at = now() WHERE id = $1 AND deleted_at IS NULL`, id)
	if err != nil {
		return fmt.Errorf("delete key: %w", err)
	}
	if tag.RowsAffected() == 0 {
		return ErrNotFound
	}
	return nil
}
