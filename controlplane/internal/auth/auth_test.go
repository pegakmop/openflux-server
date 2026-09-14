package auth

import "testing"

func TestGenerateTokenPrefixAndUniqueness(t *testing.T) {
	a, err := GenerateToken("node")
	if err != nil {
		t.Fatalf("GenerateToken: %v", err)
	}
	b, err := GenerateToken("node")
	if err != nil {
		t.Fatalf("GenerateToken: %v", err)
	}

	if a == b {
		t.Fatalf("expected distinct tokens, got the same value twice")
	}
	if len(a) < 10 {
		t.Fatalf("token looks too short: %q", a)
	}
	if a[:5] != "node_" {
		t.Fatalf("expected node_ prefix, got %q", a)
	}
}

func TestHasherDeterministicAndPepperSensitive(t *testing.T) {
	h1 := NewHasher("pepper-a")
	h2 := NewHasher("pepper-b")

	if h1.Hash("token") != h1.Hash("token") {
		t.Fatalf("hash of the same token+pepper should be deterministic")
	}
	if h1.Hash("token") == h2.Hash("token") {
		t.Fatalf("different peppers should produce different hashes for the same token")
	}
	if h1.Hash("token-a") == h1.Hash("token-b") {
		t.Fatalf("different tokens should not collide")
	}
}

func TestTokenCipherRoundTrip(t *testing.T) {
	c, err := NewTokenCipher("pepper-a")
	if err != nil {
		t.Fatalf("NewTokenCipher: %v", err)
	}
	enc, err := c.Encrypt("key_abc123")
	if err != nil {
		t.Fatalf("Encrypt: %v", err)
	}
	got, err := c.Decrypt(enc)
	if err != nil {
		t.Fatalf("Decrypt: %v", err)
	}
	if got != "key_abc123" {
		t.Errorf("Decrypt = %q, want %q", got, "key_abc123")
	}
}

func TestTokenCipherWrongPepperFails(t *testing.T) {
	c1, _ := NewTokenCipher("pepper-a")
	c2, _ := NewTokenCipher("pepper-b")

	enc, err := c1.Encrypt("secret")
	if err != nil {
		t.Fatalf("Encrypt: %v", err)
	}
	if _, err := c2.Decrypt(enc); err == nil {
		t.Errorf("expected decryption with the wrong pepper to fail")
	}
}
