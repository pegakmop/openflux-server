-- Only affects future inserts that omit max_keys at the SQL level (CreateNode always passes it
-- explicitly) - matches the app-level default in admin_handlers.go, no real limit by default.
ALTER TABLE nodes ALTER COLUMN max_keys SET DEFAULT 999999;
