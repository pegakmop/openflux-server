package gateway

import (
	"bytes"
	"encoding/binary"
	"net"
	"time"
)

// Peeking at a connection's first bytes is how the gateway learns the domain
// for a TCP connection that carries TLS: the ClientHello's SNI names the
// site, which the site policy then routes through or around the tunnel. The
// bytes are read in, parsed and replayed, so sniffing costs at most a small
// latency bump for the first round-trip - never any data loss.

// maxSniffBytes bounds how much of a connection's head is read while looking
// for a ClientHello. Real client hellos are a few hundred bytes; the cap only
// exists so an unrelated long-lived stream (e.g. plain HTTP chunking) can't
// make the gateway buffer unboundedly.
const maxSniffBytes = 2048

// peekTimeout caps how long the gateway waits for a connection's first bytes
// before giving up on sniffing it and falling back to the TLS-less path.
const peekTimeout = 750 * time.Millisecond

// prependConn replays a prefix of bytes that were already read from conn
// before the live stream - the piece that lets the gateway peek at a
// connection's head (for SNI sniffing) without losing those bytes.
type prependConn struct {
	net.Conn
	prefix *bytes.Reader
}

func (pc *prependConn) Read(p []byte) (int, error) {
	if pc.prefix.Len() > 0 {
		return pc.prefix.Read(p)
	}
	return pc.Conn.Read(p)
}

// sniffClientHello reads the head of conn looking for a TLS ClientHello,
// waiting at most peekTimeout for the first byte and reading only until one
// full TLS record is in hand (or the cap is hit). It returns the bytes read
// and a conn that replays them before the live stream. The read deadline
// that peeked is cleared before returning, so the downstream relay isn't
// left holding it; the caller still owns conn's lifecycle.
func sniffClientHello(conn net.Conn) ([]byte, net.Conn) {
	deadline := time.Now().Add(peekTimeout)
	conn.SetReadDeadline(deadline)
	collected := make([]byte, 0, maxSniffBytes)
	buf := make([]byte, 512)

	for {
		n, err := conn.Read(buf)
		collected = append(collected, buf[:n]...)
		if err != nil {
			break
		}
		// Stop early when this clearly isn't a TLS handshake record - there
		// is nothing more to gather for plain-HTTP or raw apps.
		if len(collected) < 5 || collected[0] != 0x16 {
			break
		}
		recordLen := int(binary.BigEndian.Uint16(collected[3:5]))
		if len(collected) >= 5+recordLen {
			break
		}
		if len(collected) >= maxSniffBytes || time.Now().After(deadline) {
			break
		}
	}
	conn.SetReadDeadline(time.Time{})

	if len(collected) == 0 {
		return nil, conn
	}
	return collected, &prependConn{Conn: conn, prefix: bytes.NewReader(collected)}
}