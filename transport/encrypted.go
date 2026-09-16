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
// fast. Belongs inside CompressedTransport.
//
// Strict both ways: Send always encrypts and Receive always requires a
// successful decrypt, dropping anything that isn't (see Receive). There is
// no auto-detect/passthrough fallback - a previous version of this type let
// the exit node guess per-peer, based on the client's own choice, whether to
// encrypt at all; the exit node had no way to know what the operator's
// e2e_encryption setting for that key actually was, so the setting was
// purely advisory and a client that simply didn't encrypt silently downgraded
// the connection to plaintext regardless of it. Now the caller decides
// whether to wrap in this type at all, from the SAME per-key e2e_encryption
// flag on both ends (see nodeagent.Orchestrator.startWorker and
// mobile.buildTransport) - if it says encrypt, both sides encrypt and
// anything that doesn't decrypt is dropped rather than silently accepted.
type EncryptedTransport struct {
	Transport
	send    [chacha20poly1305.KeySize]byte
	recv    [chacha20poly1305.KeySize]byte
	sendCtr atomic.Uint64
}

// NewEncryptedTransport derives separate send/receive keys from token via
// HKDF. isExitNode only picks which derived key is "ours to send with" -
// both sides must be wrapped in this together (or neither) for the same key,
// per e2e_encryption; see the type's doc comment for why there's no
// leniency for a peer that isn't.
func NewEncryptedTransport(inner Transport, token string, isExitNode bool) *EncryptedTransport {
	send, recv := DeriveDirectionalKeys(token, isExitNode, "")
	return &EncryptedTransport{Transport: inner, send: send, recv: recv}
}

// NewEncryptedTransportForStream is NewEncryptedTransport for one stream of
// a MultiStreamTransport - each stream gets its own key so independent
// per-stream nonce counters never collide under the same key.
func NewEncryptedTransportForStream(inner Transport, token string, isExitNode bool, streamIndex int) *EncryptedTransport {
	send, recv := DeriveDirectionalKeys(token, isExitNode, fmt.Sprintf(" stream %d", streamIndex))
	return &EncryptedTransport{Transport: inner, send: send, recv: recv}
}

// DeriveDirectionalKeys derives this type's send/recv keys from token via
// HKDF, picking which derived key is "ours to send with" based on
// isExitNode - both sides must derive with the same token and opposite
// isExitNode for a given connection to talk to each other. infoSuffix
// distinguishes independent keys for the same token (see
// NewEncryptedTransportForStream); pass "" for the single-stream case.
// Exported so transport/yandex can derive the exact same keys for its own
// capability-negotiated encrypted self-compression (see
// YandexDocsTransport.EnableEncryptedSelfCompression) instead of duplicating
// this derivation.
func DeriveDirectionalKeys(token string, isExitNode bool, infoSuffix string) (send, recv [chacha20poly1305.KeySize]byte) {
	base := sha256.Sum256([]byte(token))
	c2s := deriveKey(base[:], "openflux c2s"+infoSuffix)
	s2c := deriveKey(base[:], "openflux s2c"+infoSuffix)
	if isExitNode {
		return s2c, c2s
	}
	return c2s, s2c
}

func deriveKey(base []byte, info string) [chacha20poly1305.KeySize]byte {
	var out [chacha20poly1305.KeySize]byte
	io.ReadFull(hkdf.New(sha256.New, base, nil, []byte(info)), out[:])
	return out
}

// Seal encrypts data under key with a monotonic counter nonce (counter
// itself prepended in the clear, so Open doesn't need packets in order) -
// the exact wire format EncryptedTransport.Send always produced. Exported
// alongside Open so transport/yandex can reuse this exact, already-audited
// algorithm for its own encrypted self-compression instead of duplicating
// it - one implementation of the actual cryptography, used by both the
// external-wrapper path and yandex.go's internal one.
func Seal(key [chacha20poly1305.KeySize]byte, counter uint64, data []byte) ([]byte, error) {
	aead, err := chacha20poly1305.New(key[:])
	if err != nil {
		return nil, err
	}
	var nonce [chacha20poly1305.NonceSize]byte
	binary.BigEndian.PutUint64(nonce[chacha20poly1305.NonceSize-8:], counter)

	out := make([]byte, 8, 8+len(data)+aead.Overhead())
	binary.BigEndian.PutUint64(out, counter)
	return aead.Seal(out, nonce[:], data, nil), nil
}

// Open reverses Seal, returning an error for anything too short to carry a
// counter, or that fails authentication under key - never partial or
// best-effort output, matching EncryptedTransport.Receive's strict "drop
// anything that doesn't decrypt" contract.
func Open(key [chacha20poly1305.KeySize]byte, data []byte) ([]byte, error) {
	if len(data) < 8 {
		return nil, fmt.Errorf("too short to carry a nonce counter: %d bytes", len(data))
	}
	aead, err := chacha20poly1305.New(key[:])
	if err != nil {
		return nil, err
	}
	var nonce [chacha20poly1305.NonceSize]byte
	binary.BigEndian.PutUint64(nonce[chacha20poly1305.NonceSize-8:], binary.BigEndian.Uint64(data[:8]))
	return aead.Open(nil, nonce[:], data[8:], nil)
}

// Send's nonce is a monotonic counter, prepended so Receive doesn't need
// packets in order.
func (e *EncryptedTransport) Send(data []byte) error {
	out, err := Seal(e.send, e.sendCtr.Add(1), data)
	if err != nil {
		return err
	}
	return e.Transport.Send(out)
}

// Receive drops anything that doesn't decrypt with this key - a wrong token,
// a peer that isn't encrypting (old client, or e2e_encryption disagreeing
// between the two ends), or line noise - rather than passing it through. See
// the type's doc comment for why this used to be lenient on the exit-node
// side and no longer is.
func (e *EncryptedTransport) Receive(callback func([]byte)) {
	e.Transport.Receive(func(data []byte) {
		plain, err := Open(e.recv, data)
		if err != nil {
			utils.Debugf("[CRYPT] decrypt failed - dropping packet: %v", err)
			return
		}
		callback(plain)
	})
}
