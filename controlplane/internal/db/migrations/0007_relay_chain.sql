-- Lets a key's traffic hop through a second node instead of exiting directly: the entry node
-- relays over a fast encrypted UDP link (see transport.UDPRelayTransport) to final_exit_node_id,
-- which does the real internet dial. public_address is admin-provided since a node only ever
-- reaches the controlplane, never the other way around - there was previously no reason to know it.
ALTER TABLE nodes ADD COLUMN IF NOT EXISTS public_address text;

ALTER TABLE keys ADD COLUMN IF NOT EXISTS final_exit_node_id uuid REFERENCES nodes (id) ON DELETE SET NULL;
ALTER TABLE keys ADD COLUMN IF NOT EXISTS relay_port integer;

CREATE INDEX IF NOT EXISTS keys_final_exit_node_idx ON keys (final_exit_node_id) WHERE final_exit_node_id IS NOT NULL;
