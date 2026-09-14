-- doc_urls carries the yandex_multistream transport's 2+ independent Yandex
-- Docs URLs (transport.MultiStreamTransport on the Go side). doc_url stays
-- required and unused (empty string) for multistream keys: every other
-- transport still reads it, and a nullable array is simpler to check for
-- "not multistream" than special-casing an empty vs. NULL doc_url too.
ALTER TABLE keys ADD COLUMN doc_urls text[];
