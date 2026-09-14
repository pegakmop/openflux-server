package auth

import (
	"crypto/aes"
	"crypto/cipher"
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

// TokenCipher reversibly encrypts a key's raw token so it can be handed back
// to that key's assigned exit node later (for end-to-end payload
// encryption - see transport.EncryptedTransport) without storing the token
// itself in the clear. Domain-separated from Hasher's pepper use via SHA-256
// so the two derived secrets don't collide even though both start from the
// same pepper.
type TokenCipher struct {
	gcm cipher.AEAD
}

func NewTokenCipher(pepper string) (TokenCipher, error) {
	key := sha256.Sum256(append([]byte("token-cipher:"), pepper...))
	block, err := aes.NewCipher(key[:])
	if err != nil {
		return TokenCipher{}, err
	}
	gcm, err := cipher.NewGCM(block)
	if err != nil {
		return TokenCipher{}, err
	}
	return TokenCipher{gcm: gcm}, nil
}

func (c TokenCipher) Encrypt(token string) ([]byte, error) {
	nonce := make([]byte, c.gcm.NonceSize())
	if _, err := rand.Read(nonce); err != nil {
		return nil, err
	}
	return c.gcm.Seal(nonce, nonce, []byte(token), nil), nil
}

func (c TokenCipher) Decrypt(enc []byte) (string, error) {
	n := c.gcm.NonceSize()
	if len(enc) < n {
		return "", fmt.Errorf("ciphertext too short")
	}
	plain, err := c.gcm.Open(nil, enc[:n], enc[n:], nil)
	if err != nil {
		return "", err
	}
	return string(plain), nil
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
