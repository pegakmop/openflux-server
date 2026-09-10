CREATE EXTENSION IF NOT EXISTS pgcrypto;

CREATE TABLE IF NOT EXISTS nodes (
    id                uuid PRIMARY KEY DEFAULT gen_random_uuid(),
    name              text NOT NULL,
    secret_hash       text NOT NULL,
    max_keys          integer NOT NULL DEFAULT 500,
    status            text NOT NULL DEFAULT 'active',
    last_heartbeat_at timestamptz,
    created_at        timestamptz NOT NULL DEFAULT now()
);

CREATE UNIQUE INDEX IF NOT EXISTS nodes_secret_hash_idx ON nodes (secret_hash);

CREATE TABLE IF NOT EXISTS keys (
    id                  uuid PRIMARY KEY DEFAULT gen_random_uuid(),
    token_hash          text NOT NULL,
    label               text NOT NULL DEFAULT '',
    transport           text NOT NULL DEFAULT 'yandex',
    doc_url             text NOT NULL,
    assigned_node_id    uuid REFERENCES nodes (id) ON DELETE SET NULL,
    enabled             boolean NOT NULL DEFAULT true,
    traffic_limit_bytes bigint,
    bytes_sent_total    bigint NOT NULL DEFAULT 0,
    bytes_received_total bigint NOT NULL DEFAULT 0,
    owner_ref           text NOT NULL DEFAULT '',
    expires_at          timestamptz,
    created_at          timestamptz NOT NULL DEFAULT now(),
    updated_at          timestamptz NOT NULL DEFAULT now(),
    last_seen_at        timestamptz
);

CREATE UNIQUE INDEX IF NOT EXISTS keys_token_hash_idx ON keys (token_hash);
CREATE INDEX IF NOT EXISTS keys_assigned_node_enabled_idx ON keys (assigned_node_id, enabled);

CREATE TABLE IF NOT EXISTS ingest_tokens (
    id          uuid PRIMARY KEY DEFAULT gen_random_uuid(),
    token_hash  text NOT NULL,
    label       text NOT NULL DEFAULT '',
    scope       text NOT NULL DEFAULT 'keys:write',
    enabled     boolean NOT NULL DEFAULT true,
    created_at  timestamptz NOT NULL DEFAULT now()
);

CREATE UNIQUE INDEX IF NOT EXISTS ingest_tokens_token_hash_idx ON ingest_tokens (token_hash);

CREATE TABLE IF NOT EXISTS usage_daily (
    key_id         uuid NOT NULL REFERENCES keys (id) ON DELETE CASCADE,
    day            date NOT NULL,
    bytes_sent     bigint NOT NULL DEFAULT 0,
    bytes_received bigint NOT NULL DEFAULT 0,
    PRIMARY KEY (key_id, day)
);
