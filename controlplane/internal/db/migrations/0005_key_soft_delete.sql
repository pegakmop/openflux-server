-- Deleting a key used to be a hard DELETE, which took bytes_sent_total/
-- bytes_received_total with it and cascade-deleted every usage_daily row
-- for that key (its FK is ON DELETE CASCADE) - so a heavily-used key's
-- entire traffic history vanished from the panel's totals and charts the
-- moment it was deleted. Soft-deleting instead (see keys.go's DeleteKey)
-- keeps the row, and with it every historical byte count, while every
-- other query treats deleted_at IS NOT NULL as "doesn't exist".
ALTER TABLE keys ADD COLUMN deleted_at timestamptz;
