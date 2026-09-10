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
