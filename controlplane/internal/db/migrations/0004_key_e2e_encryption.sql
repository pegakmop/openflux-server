-- Per-key operator preference for the app's optional E2E payload encryption
-- (transport.EncryptedTransport, ChaCha20-Poly1305 keyed by the key's own
-- token). Exit nodes already auto-detect and interoperate either way (see
-- EncryptedTransport.autoDetect) - this column doesn't change node
-- behavior, it's what /v1/resolve and the deep link hand the app so a
-- freshly imported profile starts with the operator's intended default
-- instead of the app's own off-by-default fallback.
ALTER TABLE keys ADD COLUMN e2e_encryption boolean NOT NULL DEFAULT false;
