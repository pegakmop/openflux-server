package auth

import (
	"crypto/rand"
	"crypto/sha256"
	"crypto/subtle"
	"encoding/base64"
	"encoding/hex"
	"fmt"
	"net/http"
	"strings"
)

// GenerateToken returns a fresh high-entropy opaque token. Callers must
// store only Hash(token), never the raw value.
func GenerateToken(prefix string) (string, error) {
	raw := make([]byte, 32)
	if _, err := rand.Read(raw); err != nil {
		return "", fmt.Errorf("read random: %w", err)
	}
	encoded := base64.RawURLEncoding.EncodeToString(raw)
	if prefix != "" {
		return prefix + "_" + encoded, nil
	}
	return encoded, nil
}

// Hasher derives a deterministic, indexable digest of a token. Tokens are
// already high-entropy random values (not user-chosen passwords), so a
// peppered SHA-256 is appropriate here and keeps validation O(1) at high
// request volume instead of requiring slow password hashing.
type Hasher struct {
	pepper []byte
}

func NewHasher(pepper string) Hasher {
	return Hasher{pepper: []byte(pepper)}
}

func (h Hasher) Hash(token string) string {
	sum := sha256.Sum256(append([]byte(token), h.pepper...))
	return hex.EncodeToString(sum[:])
}

// ExtractBearer pulls the token out of an Authorization: Bearer <token> header.
func ExtractBearer(r *http.Request) (string, bool) {
	v := r.Header.Get("Authorization")
	const prefix = "Bearer "
	if !strings.HasPrefix(v, prefix) {
		return "", false
	}
	token := strings.TrimSpace(strings.TrimPrefix(v, prefix))
	if token == "" {
		return "", false
	}
	return token, true
}

// ConstantTimeEqual compares two strings without leaking timing information
// about where they first differ.
func ConstantTimeEqual(a, b string) bool {
	return subtle.ConstantTimeCompare([]byte(a), []byte(b)) == 1
}
