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
// speed, so budget phones without hardware AES aren't slower than
// flagships. Belongs INSIDE CompressedTransport (encrypt the already-
// compressed bytes).
//
// The exit-node side auto-detects instead of encrypting unconditionally: an
// old app build, or an entirely different client that was never taught
// about this, will never send ciphertext at all, and the exit node must
// keep working for it exactly as before rather than breaking the tunnel.
// The client side always encrypts once configured with a token - if it
// waited for proof first too, neither side would ever send the first
// encrypted packet.
type EncryptedTransport struct {
	Transport
	send    [chacha20poly1305.KeySize]byte
	recv    [chacha20poly1305.KeySize]byte
	sendCtr atomic.Uint64

	autoDetect   bool
	peerEncrypts atomic.Bool
}

// NewEncryptedTransport derives separate send/receive keys from token via
// HKDF (token is already 256 random bits, not a password - no slow KDF
// needed). isExitNode both picks which derived key is "ours to send with"
// and enables the auto-detect fallback described above.
func NewEncryptedTransport(inner Transport, token string, isExitNode bool) *EncryptedTransport {
	base := sha256.Sum256([]byte(token))
	c2s := deriveKey(base[:], "openflux c2s")
	s2c := deriveKey(base[:], "openflux s2c")

	e := &EncryptedTransport{Transport: inner, autoDetect: isExitNode}
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
// packets in order to reconstruct it. On the exit-node side, stays
// plaintext until this specific peer has proven (via Receive) that it
// understands encryption at all.
func (e *EncryptedTransport) Send(data []byte) error {
	if e.autoDetect && !e.peerEncrypts.Load() {
		return e.Transport.Send(data)
	}

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
		if len(data) >= 8 {
			var nonce [chacha20poly1305.NonceSize]byte
			binary.BigEndian.PutUint64(nonce[chacha20poly1305.NonceSize-8:], binary.BigEndian.Uint64(data[:8]))
			if plain, err := aead.Open(nil, nonce[:], data[8:], nil); err == nil {
				e.peerEncrypts.Store(true)
				callback(plain)
				return
			}
		}
		if e.autoDetect {
			// An old/foreign client that never encrypted at all, not a
			// corrupted packet - pass it through as-is instead of dropping
			// real traffic.
			callback(data)
			return
		}
		utils.Debugf("[CRYPT] decrypt failed")
	})
}
