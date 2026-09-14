package transport

import (
	"crypto/sha256"
	"encoding/binary"
	"fmt"
	"io"
	"sync/atomic"

	"golang.org/x/crypto/chacha20poly1305"
	"golang.org/x/crypto/hkdf"

	"universal-bypass-tool/utils"
)

// EncryptedTransport wraps another Transport with ChaCha20-Poly1305 keyed by
// a key's token - chosen over AES-GCM since it doesn't need AES-NI to be
// fast. Belongs inside CompressedTransport. The exit-node side auto-detects
// per peer instead of assuming encryption (see Receive) so an old app or a
// different client that never encrypts keeps working.
type EncryptedTransport struct {
	Transport
	send    [chacha20poly1305.KeySize]byte
	recv    [chacha20poly1305.KeySize]byte
	sendCtr atomic.Uint64

	autoDetect   bool
	peerEncrypts atomic.Bool
}

// NewEncryptedTransport derives separate send/receive keys from token via
// HKDF. isExitNode picks which derived key is "ours to send with" and
// enables the auto-detect fallback.
func NewEncryptedTransport(inner Transport, token string, isExitNode bool) *EncryptedTransport {
	return newEncryptedTransport(inner, token, isExitNode, "")
}

// NewEncryptedTransportForStream is NewEncryptedTransport for one stream of
// a MultiStreamTransport - each stream gets its own key so independent
// per-stream nonce counters never collide under the same key.
func NewEncryptedTransportForStream(inner Transport, token string, isExitNode bool, streamIndex int) *EncryptedTransport {
	return newEncryptedTransport(inner, token, isExitNode, fmt.Sprintf(" stream %d", streamIndex))
}

func newEncryptedTransport(inner Transport, token string, isExitNode bool, infoSuffix string) *EncryptedTransport {
	base := sha256.Sum256([]byte(token))
	c2s := deriveKey(base[:], "openflux c2s"+infoSuffix)
	s2c := deriveKey(base[:], "openflux s2c"+infoSuffix)

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
// packets in order. Stays plaintext until this peer proves (via Receive)
// it understands encryption.
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
			callback(data) // an old/foreign client that never encrypts, not corruption
			return
		}
		utils.Debugf("[CRYPT] decrypt failed")
	})
}
