package transport

import (
	"crypto/sha256"
	"encoding/binary"
	"io"
	"sync/atomic"

	"golang.org/x/crypto/chacha20poly1305"
	"golang.org/x/crypto/hkdf"

	"universal-bypass-tool/utils"
)

// EncryptedTransport wraps another Transport with ChaCha20-Poly1305 keyed by
// a key's token. ChaCha20 over AES-GCM: no AES-NI/ARM-crypto dependency for
// speed, so it's equally fast on budget phones without hardware AES. Belongs
// INSIDE CompressedTransport (encrypt the already-compressed bytes).
type EncryptedTransport struct {
	Transport
	send    [chacha20poly1305.KeySize]byte
	recv    [chacha20poly1305.KeySize]byte
	sendCtr atomic.Uint64
}

// NewEncryptedTransport derives separate send/receive keys from token via
// HKDF (token is already 256 random bits, not a password - no slow KDF
// needed). isExitNode picks which derived key is "ours to send with" so the
// two ends never share a nonce space.
func NewEncryptedTransport(inner Transport, token string, isExitNode bool) *EncryptedTransport {
	base := sha256.Sum256([]byte(token))
	c2s := deriveKey(base[:], "openflux c2s")
	s2c := deriveKey(base[:], "openflux s2c")

	e := &EncryptedTransport{Transport: inner}
	if isExitNode {
		e.send, e.recv = s2c, c2s
	} else {
		e.send, e.recv = c2s, s2c
	}
	return e
}

func deriveKey(base []byte, info string) [chacha20poly1305.KeySize]byte {
	var out [chacha20poly1305.KeySize]byte
	io.ReadFull(hkdf.New(sha256.New, base, nil, []byte(info)), out[:])
	return out
}

// Send's nonce is a monotonic counter, prepended so Receive doesn't need
// packets in order to reconstruct it.
func (e *EncryptedTransport) Send(data []byte) error {
	aead, err := chacha20poly1305.New(e.send[:])
	if err != nil {
		return err
	}
	ctr := e.sendCtr.Add(1)
	var nonce [chacha20poly1305.NonceSize]byte
	binary.BigEndian.PutUint64(nonce[chacha20poly1305.NonceSize-8:], ctr)

	out := make([]byte, 8, 8+len(data)+aead.Overhead())
	binary.BigEndian.PutUint64(out, ctr)
	out = aead.Seal(out, nonce[:], data, nil)
	return e.Transport.Send(out)
}

func (e *EncryptedTransport) Receive(callback func([]byte)) {
	aead, err := chacha20poly1305.New(e.recv[:])
	if err != nil {
		utils.Debugf("[CRYPT] chacha20poly1305 init failed: %v", err)
		return
	}
	e.Transport.Receive(func(data []byte) {
		if len(data) < 8 {
			return
		}
		var nonce [chacha20poly1305.NonceSize]byte
		binary.BigEndian.PutUint64(nonce[chacha20poly1305.NonceSize-8:], binary.BigEndian.Uint64(data[:8]))
		plain, err := aead.Open(nil, nonce[:], data[8:], nil)
		if err != nil {
			utils.Debugf("[CRYPT] decrypt failed: %v", err)
			return
		}
		callback(plain)
	})
}
