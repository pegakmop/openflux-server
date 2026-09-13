package store

import (
	"context"
	"errors"
	"fmt"

	"github.com/jackc/pgx/v5"

	"openflux-control/internal/model"
)

var ErrNotFound = errors.New("not found")

func (s *Store) CreateNode(ctx context.Context, name, secretHash string, maxKeys int) (model.Node, error) {
	var n model.Node
	err := s.pool.QueryRow(ctx, `
		INSERT INTO nodes (name, secret_hash, max_keys)
		VALUES ($1, $2, $3)
		RETURNING id, name, max_keys, status, last_heartbeat_at, created_at
	`, name, secretHash, maxKeys).Scan(&n.ID, &n.Name, &n.MaxKeys, &n.Status, &n.LastHeartbeatAt, &n.CreatedAt)
	if err != nil {
		return model.Node{}, fmt.Errorf("insert node: %w", err)
	}
	return n, nil
}

func (s *Store) GetNodeByID(ctx context.Context, id string) (model.Node, error) {
	var n model.Node
	err := s.pool.QueryRow(ctx, `
		SELECT id, name, max_keys, status, last_heartbeat_at, created_at
		FROM nodes WHERE id = $1
	`, id).Scan(&n.ID, &n.Name, &n.MaxKeys, &n.Status, &n.LastHeartbeatAt, &n.CreatedAt)
	if errors.Is(err, pgx.ErrNoRows) {
		return model.Node{}, ErrNotFound
	}
	if err != nil {
		return model.Node{}, fmt.Errorf("get node: %w", err)
	}
	return n, nil
}

func (s *Store) GetNodeBySecretHash(ctx context.Context, secretHash string) (model.Node, error) {
	var n model.Node
	err := s.pool.QueryRow(ctx, `
		SELECT id, name, max_keys, status, last_heartbeat_at, created_at
		FROM nodes WHERE secret_hash = $1 AND status = 'active'
	`, secretHash).Scan(&n.ID, &n.Name, &n.MaxKeys, &n.Status, &n.LastHeartbeatAt, &n.CreatedAt)
	if errors.Is(err, pgx.ErrNoRows) {
		return model.Node{}, ErrNotFound
	}
	if err != nil {
		return model.Node{}, fmt.Errorf("get node by secret: %w", err)
	}
	return n, nil
}

func (s *Store) ListNodes(ctx context.Context) ([]model.Node, error) {
	rows, err := s.pool.Query(ctx, `
		SELECT n.id, n.name, n.max_keys, n.status, n.last_heartbeat_at, n.created_at,
		       count(k.id) FILTER (WHERE k.enabled) AS active_keys
		FROM nodes n
		LEFT JOIN keys k ON k.assigned_node_id = n.id
		GROUP BY n.id
		ORDER BY n.created_at DESC
	`)
	if err != nil {
		return nil, fmt.Errorf("list nodes: %w", err)
	}
	defer rows.Close()

	var out []model.Node
	for rows.Next() {
		var n model.Node
		if err := rows.Scan(&n.ID, &n.Name, &n.MaxKeys, &n.Status, &n.LastHeartbeatAt, &n.CreatedAt, &n.ActiveKeys); err != nil {
			return nil, fmt.Errorf("scan node: %w", err)
		}
		out = append(out, n)
	}
	return out, rows.Err()
}

func (s *Store) RotateNodeSecret(ctx context.Context, id, newSecretHash string) error {
	tag, err := s.pool.Exec(ctx, `UPDATE nodes SET secret_hash = $1 WHERE id = $2`, newSecretHash, id)
	if err != nil {
		return fmt.Errorf("rotate node secret: %w", err)
	}
	if tag.RowsAffected() == 0 {
		return ErrNotFound
	}
	return nil
}

func (s *Store) TouchNodeHeartbeat(ctx context.Context, id string) error {
	tag, err := s.pool.Exec(ctx, `UPDATE nodes SET last_heartbeat_at = now() WHERE id = $1`, id)
	if err != nil {
		return fmt.Errorf("heartbeat: %w", err)
	}
	if tag.RowsAffected() == 0 {
		return ErrNotFound
	}
	return nil
}
