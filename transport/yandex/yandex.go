package yandex

import (
	"bytes"
	"encoding/base64"
	"encoding/binary"
	"encoding/json"
	"fmt"
	"hash/crc32"
	"io"
	"math"
	"math/rand"
	"net/http"
	neturl "net/url"
	"regexp"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"github.com/gorilla/websocket"
	"golang.org/x/crypto/chacha20poly1305"

	"universal-bypass-tool/transport"
	"universal-bypass-tool/utils"
)

// writerLoop batches several queued packets into one length-prefixed blob
// (Volga's framing, reused via decodeBatch) instead of one WebSocket frame
// per packet, since per-message base64/JSON/WriteMessage overhead dominates
// at higher packet rates on an exit node juggling many keys.
//
// batchMarker prefixes a batched payload so a peer can tell it apart from a
// lone compressed packet: transport.Compress only ever emits 0x00 or 0x1F as
// its own first byte, so 0xFE never collides. Sending is gated separately
// by peer capability - see kaBatchCapabilityToken.
const batchMarker = 0xFE

// kaBatchCapabilityToken rides in the keepalive so a peer only receives
// batched frames once its code is known to handle batchMarker (see
// peerBatches) - an older peer's receive path has no such check and a
// batched frame would just fail to decompress.
const kaBatchCapabilityToken = "+batch1"

// zstdBatchMarker flags EnableSelfCompression's whole-batch format: raw
// packets, length-prefixed and zstd-compressed as one unit via
// transport.EncodeBatch (see EnableSelfCompression for why this beats
// per-packet LZ4). Distinct from batchMarker and from 0x00/0x1F.
const zstdBatchMarker = 0xFD

// kaZstdBatchCapabilityToken is kaBatchCapabilityToken's counterpart for
// zstdBatchMarker (see peerZstdBatches).
const kaZstdBatchCapabilityToken = "+zbatch1"

// kaEncSelfCompressToken proves a peer specifically supports
// EnableEncryptedSelfCompression - understanding zstdBatchMarker in general
// isn't enough, since a peer can support that while still using the
// traditional CompressedTransport(EncryptedTransport(...)) wrapping for an
// e2e_encryption key. Sent only when this instance is in that mode (see
// keepAliveLoop), unlike the other two tokens which are unconditional.
const kaEncSelfCompressToken = "+encsc1"

const (
	ydocsBatchSize     = 20
	ydocsBatchTimeout  = 5 * time.Millisecond
	ydocsBatchMaxBytes = 4 * 1024 * 1024
)

// wsWriteTimeout bounds every WebSocket write - without it, a stalled write
// blocks WriteMessage forever and writerLoop (the one goroutine draining a
// session's queue for its whole life) wedges there permanently.
const wsWriteTimeout = 10 * time.Second

// defaultPingWindow is used when the server's engine.io "open" packet can't
// be parsed for its own pingInterval/pingTimeout (see performHandshake) -
// 25s+20s matches Socket.IO's own common server-side defaults.
const defaultPingWindow = 45 * time.Second

// handshakeTimeout bounds how long connectToDoc waits for the engine.io
// open packet and the socket.io namespace-connect ack before giving up and
// reconnecting - see the package-level doc comment on performHandshake for
// why this handshake has to be awaited at all.
const handshakeTimeout = 15 * time.Second

// Reason codes passed to scheduleReconnect and reported as the last field of
// transport.EventRetrying's detail - see mobile.Callback.OnLogEvent and the
// Android app's Logs tab for how these map to human-readable text.
const (
	reasonFetchFailed     = "fetch_failed"
	reasonDialFailed      = "dial_failed"
	reasonHandshakeFailed = "handshake_failed"
	reasonSendFailed      = "send_failed"
	reasonReadError       = "read_error"
)

type YandexDocsInfo struct {
	CookieStr   string
	Token       string
	DocID       string
	CallbackURL string
	UserID      string
	Origin      string
	Host        string
	WsURL       string
	Permissions map[string]interface{}
	OpenCmd     map[string]interface{}
}

type DocSession struct {
	Info       YandexDocsInfo
	Conn       *websocket.Conn
	WriteQueue chan []byte
	UserID     string
	writeMu    sync.Mutex
}

func (s *DocSession) safeWrite(messageType int, data []byte) error {
	s.writeMu.Lock()
	defer s.writeMu.Unlock()
	s.Conn.SetWriteDeadline(time.Now().Add(wsWriteTimeout))
	err := s.Conn.WriteMessage(messageType, data)
	if err != nil {
		s.Conn.Close() // let the read loop notice and reconnect
	}
	return err
}

type YandexDocsTransport struct {
	*transport.BaseTransport

	url     string
	session *DocSession

	userCounter atomic.Int32
	baseUserID  string

	// recentSent guards against self-echo: Yandex's doc broadcasts every
	// "cursor" event to every participant including the sender, so a packet
	// we just sent (see writerLoop) would otherwise come back through
	// handleMessage and get reinjected as if the peer had sent it. A
	// short-lived hash of every sent payload lets an inbound match be
	// dropped without depending on Yandex's broadcast wrapping format.
	recentSentMu sync.Mutex
	recentSent   map[uint32]time.Time

	// peerBatches is learned from the peer's keepalive (kaBatchCapabilityToken):
	// only send batched frames once the peer is known to parse batchMarker.
	peerBatches atomic.Bool

	// peerZstdBatches is peerBatches' counterpart for zstdBatchMarker.
	peerZstdBatches atomic.Bool

	// selfCompress/encrypted/encSend/encRecv are set at most once before
	// Start and never written again (same contract as SetEventCallback),
	// so reading them from writerLoop/handleMessage without a lock is safe.
	selfCompress bool

	// encrypted, encSend, encRecv, encSendCtr hold
	// EnableEncryptedSelfCompression's state: encSend/encRecv are this
	// connection's derived ChaCha20-Poly1305 keys, encSendCtr this side's
	// nonce counter - independent of any external EncryptedTransport.
	encrypted  bool
	encSend    [chacha20poly1305.KeySize]byte
	encRecv    [chacha20poly1305.KeySize]byte
	encSendCtr atomic.Uint64

	// peerEncSelfCompress proves a peer specifically supports encrypted
	// self-compression; peerZstdBatches alone isn't enough (see
	// kaEncSelfCompressToken).
	peerEncSelfCompress atomic.Bool

	// wakeReconnect, guarded by Mu, is non-nil exactly while scheduleReconnect
	// sleeps out a backoff delay - see ForceReconnect.
	wakeReconnect chan struct{}
}

// EnableSelfCompression switches this transport to managing its own
// per-batch compression (see writerLoop/handleMessage) instead of expecting
// a caller-side transport.CompressedTransport to compress each packet
// before Send. Call before Start; not safe to change afterward.
//
// Per-packet LZ4 (transport.CompressedTransport) misses redundancy between
// packets in the same batch and pays a full LZ4 frame's overhead each time.
// Once a peer's keepalive proves it understands zstdBatchMarker, writerLoop
// instead zstd-compresses a whole batch of raw packets as one unit
// (transport.EncodeBatch); until then it falls back to reproducing
// transport.CompressedTransport's per-packet wire format so an unmodified
// peer stays compatible.
//
// Must not be combined with wrapping this transport in
// transport.CompressedTransport (packets would be compressed twice). For an
// e2e_encryption key, use EnableEncryptedSelfCompression instead - a plain
// transport.EncryptedTransport wrapper would encrypt this method's batching
// decisions like opaque bytes and defeat the point.
func (t *YandexDocsTransport) EnableSelfCompression() {
	t.selfCompress = true
}

// EnableEncryptedSelfCompression is EnableSelfCompression plus end-to-end
// encryption, for a key with e2e_encryption on. token/isExitNode derive the
// same keys transport.NewEncryptedTransport would (transport.DeriveDirectionalKeys);
// both ends must call this with the same token and opposite isExitNode.
// Call before Start; not safe to change afterward.
//
// This needs its own method rather than wrapping EnableSelfCompression's
// output in transport.EncryptedTransport externally: encrypting each packet
// before batching destroys the cross-packet redundancy batching raw packets
// was meant to exploit (ciphertext is high-entropy, so zstd finds nothing to
// compress). Instead this batches and zstd-compresses RAW packets first,
// then encrypts the whole compressed batch as one unit (transport.Seal) -
// encryption still only ever seals already-compressed bytes.
//
// Understanding zstdBatchMarker in general does NOT prove a peer supports
// ENCRYPTED self-compression for this key - a peer can support the former
// while still using the traditional CompressedTransport(EncryptedTransport(...))
// wrapping for an e2e_encryption key - so this is gated by its own token,
// kaEncSelfCompressToken. Until a peer proves it, this reproduces that
// traditional wrapping's exact wire format (encrypt each compressed packet
// independently, then batch the ciphertexts) so an unmodified peer using
// the external-wrapper architecture stays compatible. Receiving is
// unconditional: this side can decrypt either format from any peer sharing
// the same token, regardless of which format the peer sends.
func (t *YandexDocsTransport) EnableEncryptedSelfCompression(token string, isExitNode bool) {
	t.enableEncryptedSelfCompression(token, isExitNode, "")
}

// EnableEncryptedSelfCompressionForStream is
// EnableEncryptedSelfCompression for one stream of a MultiStreamTransport -
// see transport.NewEncryptedTransportForStream's doc comment on why each
// stream needs its own derived key.
func (t *YandexDocsTransport) EnableEncryptedSelfCompressionForStream(token string, isExitNode bool, streamIndex int) {
	t.enableEncryptedSelfCompression(token, isExitNode, fmt.Sprintf(" stream %d", streamIndex))
}

func (t *YandexDocsTransport) enableEncryptedSelfCompression(token string, isExitNode bool, infoSuffix string) {
	t.selfCompress = true
	t.encrypted = true
	t.encSend, t.encRecv = transport.DeriveDirectionalKeys(token, isExitNode, infoSuffix)
}

func NewYandexDocsTransport(url string, config transport.TransportConfig) *YandexDocsTransport {
	t := &YandexDocsTransport{
		BaseTransport: transport.NewBaseTransport(config),
		url:           normalizeDocURL(url),
	}
	t.baseUserID = randUserID()
	return t
}

// normalizeDocURL rewrites a Yandex Disk share link ("disk.yandex.ru/i/<hash>")
// to the equivalent docs.yandex.ru URL fetchDocInfo expects - both serve the
// same client-config page, just under different hostnames. Anything else
// passes through unchanged.
func normalizeDocURL(raw string) string {
	u, err := neturl.Parse(raw)
	if err != nil {
		return raw
	}
	if strings.EqualFold(u.Hostname(), "disk.yandex.ru") && strings.HasPrefix(u.Path, "/i/") {
		u.Host = "docs.yandex.ru"
		return u.String()
	}
	return raw
}

func (t *YandexDocsTransport) Start() error {
	if err := t.BaseTransport.Start(); err != nil {
		return err
	}

	t.baseUserID = randUserID()
	go t.keepAliveLoop()
	t.connectToDoc(0)

	return nil
}

// Send queues data for the writer loop to put on the wire. Deliberately
// does not require IsConnected(): a session's WriteQueue is reused across a
// reconnect (see connectToDoc) specifically so a brief drop can queue data
// instead of dropping it and forcing the real end-to-end TCP connection to
// notice the loss and retransmit on its own, much slower, timeout.
func (t *YandexDocsTransport) Send(data []byte) error {
	t.Mu.RLock()
	session := t.session
	t.Mu.RUnlock()

	if session == nil {
		return fmt.Errorf("no active session")
	}

	select {
	case session.WriteQueue <- data:
		t.RecordSend(len(data))
		return nil
	default:
		return fmt.Errorf("write queue full")
	}
}

func (t *YandexDocsTransport) connectToDoc(attempt int) {
	if !t.IsRunning() {
		return
	}

	utils.Debugf("[YDOCS] connectToDoc attempt %d", attempt)
	t.EmitEvent(transport.EventConnecting, strconv.Itoa(attempt+1))

	go func() {
		t.Mu.Lock()
		existingSession := t.session
		t.Mu.Unlock()

		var userID string
		if existingSession != nil {
			userID = existingSession.UserID
		} else {
			suffix := fmt.Sprintf("%03d", t.userCounter.Add(1)%1000)
			userID = t.baseUserID + suffix
		}

		info, err := t.fetchDocInfo(t.url, userID)
		if err != nil {
			utils.Debugf("[YDOCS] fetchDocInfo failed: %v", err)
			t.scheduleReconnect(attempt, reasonFetchFailed, err)
			return
		}

		dialer := websocket.Dialer{
			HandshakeTimeout:  10 * time.Second,
			EnableCompression: true, // negotiated (permessage-deflate); harmless if the server ignores it
			NetDialContext:    transport.ProtectedDialer().DialContext,
		}
		headers := http.Header{}
		headers.Set("User-Agent", browserUserAgent)
		headers.Set("Origin", info.Origin)
		headers.Set("Cookie", info.CookieStr)
		headers.Set("Host", info.Host)
		// WebSocket upgrade, not a page load - real Firefox sends these
		// Sec-Fetch-* values for a same-origin WS opened from a loaded page.
		headers.Set("Sec-Fetch-Dest", "websocket")
		headers.Set("Sec-Fetch-Mode", "websocket")
		headers.Set("Sec-Fetch-Site", "same-origin")

		conn, _, err := dialer.Dial(info.WsURL, headers)
		if err != nil {
			utils.Debugf("[YDOCS] WebSocket dial failed: %v", err)
			t.scheduleReconnect(attempt, reasonDialFailed, err)
			return
		}

		// The engine.io/socket.io handshake must complete before anything
		// else goes over this socket - sending data ahead of the server's
		// connect ack lands it in an unconfirmed namespace, which makes
		// OnlyOffice's backend tear the connection down with close code 1005.
		readTimeout, err := t.performHandshake(conn, info.Token)
		if err != nil {
			utils.Debugf("[YDOCS] handshake failed: %v", err)
			conn.Close()
			t.scheduleReconnect(attempt, reasonHandshakeFailed, err)
			return
		}

		writeQueue := make(chan []byte, t.GetConfig().MaxQueueSize)
		if existingSession != nil {
			writeQueue = existingSession.WriteQueue
		}

		session := &DocSession{
			Info:       info,
			Conn:       conn,
			WriteQueue: writeQueue,
			UserID:     userID,
		}

		t.Mu.Lock()
		t.session = session
		t.SetConnected(true)
		t.Mu.Unlock()

		if existingSession == nil {
			go t.writerLoop(writeQueue)
		}

		authData := map[string]interface{}{
			"type": "auth", "docid": info.DocID, "token": "fghhfgsjdgfjs",
			"user": map[string]interface{}{"id": userID}, "editorType": 0,
			"lastOtherSaveTime": -1, "permissions": info.Permissions,
			"openCmd": info.OpenCmd, "coEditingMode": "fast", "jwtOpen": info.Token,
		}
		messagePart, err := json.Marshal([]interface{}{"message", authData})
		if err != nil {
			utils.Debugf("[YDOCS] marshal auth message failed: %v", err)
			t.SetConnected(false)
			conn.Close()
			t.scheduleReconnect(attempt, reasonSendFailed, err)
			return
		}
		if err := session.safeWrite(websocket.TextMessage, []byte(fmt.Sprintf("42%s", string(messagePart)))); err != nil {
			utils.Debugf("[YDOCS] send auth message failed: %v", err)
			t.SetConnected(false)
			conn.Close()
			t.scheduleReconnect(attempt, reasonSendFailed, err)
			return
		}
		t.EmitEvent(transport.EventConnected, strconv.Itoa(attempt+1))
		connectedAt := time.Now()

		for t.IsRunning() {
			conn.SetReadDeadline(time.Now().Add(readTimeout))
			_, message, err := conn.ReadMessage()
			if err != nil {
				utils.Debugf("[YDOCS] Read error: %v", err)
				t.SetConnected(false)
				conn.Close()
				// A session that stayed up a while before dropping is a
				// normal blip, not evidence backoff should keep growing -
				// otherwise a long-lived transport's backoff ratchets up to
				// MaxReconnectDelay and stays there for every future drop.
				next := attempt
				if time.Since(connectedAt) > 15*time.Second {
					next = 0
				}
				t.scheduleReconnect(next, reasonReadError, err)
				return
			}
			t.handleMessage(session, message)
		}
	}()
}

// performHandshake waits out the engine.io/socket.io connection sequence:
//
//  1. server -> client: engine.io "open" packet ("0{...}"), carrying the
//     server's pingInterval/pingTimeout;
//  2. client -> server: socket.io namespace-connect ("40{"token":...}"),
//     sent only once (1) has arrived;
//  3. server -> client: namespace-connect ack ("40{"sid":...}") or a
//     connect-error ("44...") - the socket is usable only after the ack.
//
// Engine.io pings ("2") can arrive at any point and are answered ("3")
// immediately regardless of handshake progress. Returns the read-idle
// timeout for the rest of this connection's life, derived from the
// server's ping settings so a silently-dead connection is detected.
func (t *YandexDocsTransport) performHandshake(conn *websocket.Conn, token string) (time.Duration, error) {
	conn.SetReadDeadline(time.Now().Add(handshakeTimeout))
	defer conn.SetReadDeadline(time.Time{})

	readTimeout := defaultPingWindow
	sentConnect := false

	for {
		_, msg, err := conn.ReadMessage()
		if err != nil {
			return 0, fmt.Errorf("handshake read: %w", err)
		}
		text := string(msg)

		switch {
		case !sentConnect && strings.HasPrefix(text, "0"):
			var openPkt struct {
				PingInterval int `json:"pingInterval"`
				PingTimeout  int `json:"pingTimeout"`
			}
			if err := json.Unmarshal([]byte(text[1:]), &openPkt); err == nil &&
				openPkt.PingInterval > 0 && openPkt.PingTimeout > 0 {
				readTimeout = time.Duration(openPkt.PingInterval+openPkt.PingTimeout) * time.Millisecond
			}

			authPkt, err := json.Marshal(map[string]string{"token": token})
			if err != nil {
				return 0, fmt.Errorf("marshal namespace-connect: %w", err)
			}
			conn.SetWriteDeadline(time.Now().Add(wsWriteTimeout))
			if err := conn.WriteMessage(websocket.TextMessage, append([]byte("40"), authPkt...)); err != nil {
				return 0, fmt.Errorf("send namespace-connect: %w", err)
			}
			sentConnect = true

		case text == "2":
			conn.SetWriteDeadline(time.Now().Add(wsWriteTimeout))
			if err := conn.WriteMessage(websocket.TextMessage, []byte("3")); err != nil {
				return 0, fmt.Errorf("pong during handshake: %w", err)
			}

		case strings.HasPrefix(text, "44"):
			return 0, fmt.Errorf("namespace connect rejected: %s", text)

		case sentConnect && strings.HasPrefix(text, "40"):
			return readTimeout, nil

		default:
			utils.Debugf("[YDOCS] unexpected message during handshake: %s", text)
		}
	}
}

func (t *YandexDocsTransport) writerLoop(queue chan []byte) {
	batch := make([][]byte, 0, ydocsBatchSize)
	totalBytes := 0

	flush := func(session *DocSession) {
		if len(batch) == 0 {
			return
		}
		switch {
		case t.encrypted && t.peerEncSelfCompress.Load():
			// batch holds RAW packets - frame+zstd-compress as one unit,
			// then encrypt that as one unit (EnableEncryptedSelfCompression).
			t.sendEncryptedZstdBatch(session, batch)
		case t.encrypted:
			// Peer hasn't proven encrypted self-compression for this key -
			// reproduce CompressedTransport(EncryptedTransport(...))'s exact
			// per-packet format (compress then encrypt each independently)
			// so an unmodified peer using that wrapping stays compatible.
			sealed := make([][]byte, 0, len(batch))
			for _, pkt := range batch {
				ciphertext, err := transport.Seal(t.encSend, t.encSendCtr.Add(1), transport.Compress(pkt))
				if err != nil {
					utils.Debugf("[YDOCS] encrypt failed, dropping packet: %v", err)
					continue
				}
				sealed = append(sealed, ciphertext)
			}
			if len(sealed) > 0 {
				if t.peerBatches.Load() {
					t.sendBatch(session, sealed)
				} else {
					for _, pkt := range sealed {
						t.sendSingle(session, pkt)
					}
				}
			}
		case t.selfCompress && t.peerZstdBatches.Load():
			// batch holds RAW packets (nothing upstream compressed them) -
			// frame and zstd-compress the whole thing as one unit.
			t.sendZstdBatch(session, batch)
		case t.selfCompress:
			// Peer hasn't proven zstdBatchMarker support - reproduce
			// transport.CompressedTransport's per-packet format so the wire
			// bytes match what an unmodified peer expects.
			compressed := make([][]byte, len(batch))
			for i, pkt := range batch {
				compressed[i] = transport.Compress(pkt)
			}
			if t.peerBatches.Load() {
				t.sendBatch(session, compressed)
			} else {
				for _, pkt := range compressed {
					t.sendSingle(session, pkt)
				}
			}
		case t.peerBatches.Load():
			t.sendBatch(session, batch)
		default:
			for _, pkt := range batch {
				t.sendSingle(session, pkt)
			}
		}
		batch = batch[:0]
		totalBytes = 0
	}

	for t.IsRunning() {
		// t.session is never nil'd on disconnect (see connectToDoc) - it
		// keeps pointing at the old, dead session until replaced, so a nil
		// check alone never catches a drop. IsConnected() is what actually
		// keeps queued data queued until a live session exists to drain it.
		t.Mu.RLock()
		session := t.session
		connected := t.IsConnected()
		t.Mu.RUnlock()
		if session == nil || session.Conn == nil || !connected {
			time.Sleep(50 * time.Millisecond)
			continue
		}

		select {
		case packet := <-queue:
			batch = append(batch, packet)
			totalBytes += len(packet)
			if len(batch) >= ydocsBatchSize || totalBytes >= ydocsBatchMaxBytes {
				flush(session)
			}
		case <-time.After(ydocsBatchTimeout):
			// Whatever's accumulated so far (even a single packet) goes
			// out now rather than waiting for a full batch - low traffic
			// must not turn into added latency.
			flush(session)
		}
	}
}

// sendBatch frames batch as one length-prefixed blob (batchMarker + Volga's
// [len,data]... encoding, decoded via decodeBatch), base64s it, and writes
// it as a single "cursor" message.
func (t *YandexDocsTransport) sendBatch(session *DocSession, batch [][]byte) {
	var blob bytes.Buffer
	blob.WriteByte(batchMarker)
	var lenBuf [2]byte
	for _, p := range batch {
		binary.BigEndian.PutUint16(lenBuf[:], uint16(len(p)))
		blob.Write(lenBuf[:])
		blob.Write(p)
	}
	framed := blob.Bytes()

	t.markSent(framed)
	if utils.IsVerbose() {
		// batch's items are already compressed/sealed opaque bytes here, not
		// raw IP packets - only byte/packet counts are safe to log.
		utils.Debugf("[YDOCS] -> %d bytes (%d packets)\n", len(framed), len(batch))
	}

	payload := base64.StdEncoding.EncodeToString(framed)
	msg := fmt.Sprintf(`42["message",{"type":"cursor","cursor":"18;%s"}]`, payload)

	if err := session.safeWrite(websocket.TextMessage, []byte(msg)); err != nil {
		utils.Debugf("[YDOCS] Write error: %v", err)
	}
}

// sendZstdBatch frames batch (raw packets) with zstdBatchMarker +
// transport.EncodeBatch's zstd encoding. Only called once the peer's
// keepalive has proven it understands zstdBatchMarker.
func (t *YandexDocsTransport) sendZstdBatch(session *DocSession, batch [][]byte) {
	encoded := transport.EncodeBatch(batch)
	framed := make([]byte, 1+len(encoded))
	framed[0] = zstdBatchMarker
	copy(framed[1:], encoded)

	t.markSent(framed)
	if utils.IsVerbose() {
		utils.Debugf("[YDOCS] -> %d bytes (%d packets, zstd batch)\n", len(framed), len(batch))
	}

	payload := base64.StdEncoding.EncodeToString(framed)
	msg := fmt.Sprintf(`42["message",{"type":"cursor","cursor":"18;%s"}]`, payload)

	if err := session.safeWrite(websocket.TextMessage, []byte(msg)); err != nil {
		utils.Debugf("[YDOCS] Write error: %v", err)
	}
}

// sendEncryptedZstdBatch is sendZstdBatch plus encryption: the
// [zstdBatchMarker][EncodeBatch(batch)] plaintext sealed as one unit. Only
// called once the peer's keepalive proves it understands this format.
func (t *YandexDocsTransport) sendEncryptedZstdBatch(session *DocSession, batch [][]byte) {
	encoded := transport.EncodeBatch(batch)
	plaintext := make([]byte, 1+len(encoded))
	plaintext[0] = zstdBatchMarker
	copy(plaintext[1:], encoded)

	ciphertext, err := transport.Seal(t.encSend, t.encSendCtr.Add(1), plaintext)
	if err != nil {
		utils.Debugf("[YDOCS] encrypt failed, dropping batch: %v", err)
		return
	}

	t.markSent(ciphertext)
	if utils.IsVerbose() {
		utils.Debugf("[YDOCS] -> %d bytes (%d packets, encrypted zstd batch)\n", len(ciphertext), len(batch))
	}

	payload := base64.StdEncoding.EncodeToString(ciphertext)
	msg := fmt.Sprintf(`42["message",{"type":"cursor","cursor":"18;%s"}]`, payload)

	if err := session.safeWrite(websocket.TextMessage, []byte(msg)); err != nil {
		utils.Debugf("[YDOCS] Write error: %v", err)
	}
}

// sendSingle is the legacy one-packet-per-message format, used until the
// peer's keepalive proves it understands batched frames (see peerBatches).
func (t *YandexDocsTransport) sendSingle(session *DocSession, packet []byte) {
	t.markSent(packet)
	if utils.IsVerbose() {
		utils.Debugf("[YDOCS] -> %d bytes (unbatched)\n", len(packet))
	}

	payload := base64.StdEncoding.EncodeToString(packet)
	msg := fmt.Sprintf(`42["message",{"type":"cursor","cursor":"18;%s"}]`, payload)

	if err := session.safeWrite(websocket.TextMessage, []byte(msg)); err != nil {
		utils.Debugf("[YDOCS] Write error: %v", err)
	}
}

// markSent records that data was just sent so a later self-echo can be
// recognized and dropped (see recentSent). Entries age out opportunistically
// here rather than needing an explicit cap: an echo either arrives within a
// couple seconds or not at all.
func (t *YandexDocsTransport) markSent(data []byte) {
	h := crc32.ChecksumIEEE(data)
	now := time.Now()

	t.recentSentMu.Lock()
	defer t.recentSentMu.Unlock()
	if t.recentSent == nil {
		t.recentSent = make(map[uint32]time.Time)
	}
	t.recentSent[h] = now
	if len(t.recentSent) > 512 {
		cutoff := now.Add(-5 * time.Second)
		for k, ts := range t.recentSent {
			if ts.Before(cutoff) {
				delete(t.recentSent, k)
			}
		}
	}
}

// wasRecentlySent reports whether data matches something markSent recorded
// in the last 5 seconds - well over one round trip to Yandex's servers, so
// this only matches a genuine self-echo (a coincidental CRC32 collision is
// astronomically unlikely).
func (t *YandexDocsTransport) wasRecentlySent(data []byte) bool {
	h := crc32.ChecksumIEEE(data)

	t.recentSentMu.Lock()
	ts, ok := t.recentSent[h]
	t.recentSentMu.Unlock()

	return ok && time.Since(ts) < 5*time.Second
}

func (t *YandexDocsTransport) keepAliveLoop() {
	ticker := time.NewTicker(t.GetConfig().KeepAliveInterval)
	defer ticker.Stop()
	// Capability tokens ride inside the "---KA---" keepalive as substrings, so
	// a peer recognizing only some (or none) still matches "---KA---" and
	// whatever it knows. kaEncSelfCompressToken is conditional on t.encrypted
	// (it must prove THIS instance is in encrypted self-compression mode);
	// the other two are sent unconditionally.
	keepAliveMsg := `42["message",{"type":"cursor","cursor":"18;---KA---` + kaBatchCapabilityToken + kaZstdBatchCapabilityToken
	if t.encrypted {
		keepAliveMsg += kaEncSelfCompressToken
	}
	keepAliveMsg += `"}]`

	for t.IsRunning() {
		<-ticker.C
		t.Mu.Lock()
		session := t.session
		t.Mu.Unlock()

		if session != nil && session.Conn != nil {
			if err := session.safeWrite(websocket.TextMessage, []byte(keepAliveMsg)); err != nil {
				utils.Debugf("[YDOCS] Keep-alive failed: %v", err)
				t.SetConnected(false)
				// Unblocks the read loop's ReadMessage() immediately instead
				// of waiting out the full read-deadline window.
				session.Conn.Close()
			}
		}
	}
}

func (t *YandexDocsTransport) handleMessage(session *DocSession, data []byte) {
	text := string(data)

	if strings.Contains(text, "---KA---") {
		if strings.Contains(text, kaBatchCapabilityToken) {
			t.peerBatches.Store(true)
		}
		if strings.Contains(text, kaZstdBatchCapabilityToken) {
			t.peerZstdBatches.Store(true)
		}
		if strings.Contains(text, kaEncSelfCompressToken) {
			t.peerEncSelfCompress.Store(true)
		}
		return
	}

	if text == "2" {
		if session != nil && session.Conn != nil {
			session.safeWrite(websocket.TextMessage, []byte("3"))
		}
		return
	}
	if text == "3" {
		return
	}

	if strings.Contains(text, "saveChanges") || strings.Contains(text, "cursor") {
		base64Str := t.extractBase64String(text)
		if base64Str == "" {
			return
		}

		decoded, err := base64.StdEncoding.DecodeString(base64Str)
		if err != nil {
			utils.Debugf("[YDOCS] Base64 decode error: %v", err)
			return
		}

		// Self-echo: the doc broadcasts every cursor event to every
		// participant including the sender (see recentSent).
		if t.wasRecentlySent(decoded) {
			if utils.IsVerbose() {
				utils.Debugf("[YDOCS] dropped self-echo (%d bytes)\n", len(decoded))
			}
			return
		}

		if utils.IsVerbose() {
			utils.Debugf("[YDOCS] <- %d bytes\n", len(decoded))
		}

		t.RecordReceive(len(decoded))

		if t.encrypted {
			t.handleEncryptedMessage(decoded)
			return
		}

		// zstdBatchMarker flags decoded as EnableSelfCompression's whole-batch
		// format (raw packets, zstd-compressed as one unit). Decoding it
		// doesn't depend on our own t.selfCompress - a peer capable of
		// sending it is by construction capable of receiving plain packets.
		if len(decoded) > 0 && decoded[0] == zstdBatchMarker {
			pkts, err := transport.DecodeBatch(decoded[1:])
			if err != nil {
				utils.Debugf("[YDOCS] zstd batch decode error: %v", err)
				return
			}
			for _, pkt := range pkts {
				t.CallReceive(pkt)
			}
			return
		}

		// batchMarker flags decoded as several length-prefixed
		// transport.Compress'd packets. In self-compress mode there's no
		// external CompressedTransport to decompress them, so this does it
		// before handing packets upward; otherwise the caller's own
		// CompressedTransport.Receive expects to do that itself.
		if len(decoded) > 0 && decoded[0] == batchMarker {
			for _, pkt := range decodeBatch(decoded[1:]) {
				if !t.selfCompress {
					t.CallReceive(pkt)
					continue
				}
				raw, err := transport.Decompress(pkt)
				if err != nil {
					utils.Debugf("[YDOCS] batch item decompress error: %v", err)
					continue
				}
				t.CallReceive(raw)
			}
			return
		}

		if !t.selfCompress {
			t.CallReceive(decoded)
			return
		}
		raw, err := transport.Decompress(decoded)
		if err != nil {
			utils.Debugf("[YDOCS] decompress error: %v", err)
			return
		}
		t.CallReceive(raw)
	}
}

// handleEncryptedMessage dispatches decoded wire bytes for an
// EnableEncryptedSelfCompression'd transport - every path ends in a decrypt,
// but at a different point depending on which wire format arrived:
//
//   - the legacy per-item fallback has an UNENCRYPTED batchMarker wrapping
//     individually encrypted items, so each is decrypted separately;
//   - the whole-batch format and the old single-item fallback are both
//     entirely ciphertext, distinguishable only after decrypting (by
//     zstdBatchMarker as the plaintext's first byte, or not).
//
// Decrypting the wrong span would just fail authentication and drop
// everything, which is what this dispatch exists to avoid.
func (t *YandexDocsTransport) handleEncryptedMessage(decoded []byte) {
	if len(decoded) > 0 && decoded[0] == batchMarker {
		for _, item := range decodeBatch(decoded[1:]) {
			plain, err := transport.Open(t.encRecv, item)
			if err != nil {
				utils.Debugf("[YDOCS] batch item decrypt failed: %v", err)
				continue
			}
			raw, err := transport.Decompress(plain)
			if err != nil {
				utils.Debugf("[YDOCS] batch item decompress error: %v", err)
				continue
			}
			t.CallReceive(raw)
		}
		return
	}

	plaintext, err := transport.Open(t.encRecv, decoded)
	if err != nil {
		utils.Debugf("[YDOCS] decrypt failed - dropping message: %v", err)
		return
	}

	if len(plaintext) > 0 && plaintext[0] == zstdBatchMarker {
		pkts, err := transport.DecodeBatch(plaintext[1:])
		if err != nil {
			utils.Debugf("[YDOCS] encrypted zstd batch decode error: %v", err)
			return
		}
		for _, pkt := range pkts {
			t.CallReceive(pkt)
		}
		return
	}

	raw, err := transport.Decompress(plaintext)
	if err != nil {
		utils.Debugf("[YDOCS] decompress error: %v", err)
		return
	}
	t.CallReceive(raw)
}

func (t *YandexDocsTransport) extractBase64String(response string) string {
	if strings.Contains(response, "saveChanges") {
		marker := `"excelAdditionalInfo":"`
		left := strings.Index(response, marker) + len(marker)
		if left < len(marker) {
			return ""
		}
		right := strings.Index(response[left:], `"`)
		if right == -1 {
			return ""
		}
		return response[left : left+right]
	}

	re := regexp.MustCompile(`"cursor":"[^;]+;([^"]+)"`)
	matches := re.FindStringSubmatch(response)
	if len(matches) > 1 {
		return matches[1]
	}
	return ""
}

// scheduleReconnect waits out an exponential backoff before retrying,
// instead of hammering the server in a tight loop on every failed attempt.
// cause carries the actual error alongside reasonCode's fixed category, for
// a caller that wants to show both.
func (t *YandexDocsTransport) scheduleReconnect(attempt int, reasonCode string, cause error) {
	if !t.IsRunning() || attempt >= t.GetConfig().MaxReconnectAttempts {
		return
	}

	t.RecordReconnect()

	delay := t.backoffDelay(attempt)
	causeText := strings.ReplaceAll(cause.Error(), "\n", " ")
	t.EmitEvent(transport.EventRetrying, fmt.Sprintf("%d|%d|%s|%s", attempt+1, int(delay.Seconds()), reasonCode, causeText))
	if delay > 0 {
		// wake lets ForceReconnect cut this short - published under Mu so a
		// concurrent ForceReconnect either sees and closes it, or arrives
		// too early/late to matter.
		wake := make(chan struct{})
		t.Mu.Lock()
		t.wakeReconnect = wake
		t.Mu.Unlock()

		select {
		case <-time.After(delay):
		case <-wake:
			utils.Debugf("[YDOCS] backoff wait cut short by ForceReconnect")
		}

		t.Mu.Lock()
		if t.wakeReconnect == wake {
			t.wakeReconnect = nil
		}
		t.Mu.Unlock()
	}
	if !t.IsRunning() {
		return
	}

	t.connectToDoc(attempt + 1)
}

// ForceReconnect makes the transport retry right now: drops a live
// connection so its read loop redials through scheduleReconnect, or, if
// instead sleeping out a backoff delay, cuts that wait short. No-op if
// neither applies.
//
// A network change (Wi-Fi to mobile data) often leaves the old socket
// silently dead rather than reset - nothing notices until a read times out,
// far later than the backoff delay this skips. A caller that already knows
// the network changed can report that here instead of waiting for TCP.
func (t *YandexDocsTransport) ForceReconnect() {
	t.Mu.Lock()
	session := t.session
	// t.session is never nil'd on disconnect, so session != nil alone can't
	// distinguish "connected now" from "backoff sleep with a stale session".
	live := t.IsConnected()
	wake := t.wakeReconnect
	t.wakeReconnect = nil // claimed under the lock so a concurrent call can't double-close wake
	t.Mu.Unlock()

	if live && session != nil && session.Conn != nil {
		utils.Debugf("[YDOCS] force-reconnect: dropping live session to re-dial")
		_ = session.Conn.Close()
		return
	}
	if wake != nil {
		close(wake)
	}
}

func (t *YandexDocsTransport) backoffDelay(attempt int) time.Duration {
	cfg := t.GetConfig()
	if cfg.ReconnectDelay <= 0 {
		return 0
	}

	multiplier := cfg.ReconnectMultiplier
	if multiplier < 1 {
		multiplier = 1
	}

	delay := float64(cfg.ReconnectDelay) * math.Pow(multiplier, float64(attempt))
	// +0-50% jitter, applied before the cap: every reconnect registers as a
	// brand new participant in Yandex's doc-collab room regardless of
	// user-id reuse, so many clients losing the same document at once would
	// otherwise retry in lockstep and pile up "ghost" participants.
	delay += delay * 0.5 * rand.Float64()
	if cfg.MaxReconnectDelay > 0 && delay > float64(cfg.MaxReconnectDelay) {
		delay = float64(cfg.MaxReconnectDelay)
	}
	return time.Duration(delay)
}

func (t *YandexDocsTransport) fetchDocInfo(url, userID string) (YandexDocsInfo, error) {
	client := &http.Client{
		CheckRedirect: func(req *http.Request, via []*http.Request) error { return nil },
		Timeout:       30 * time.Second,
		Transport:     &http.Transport{DialContext: transport.ProtectedDialer().DialContext},
	}

	req, _ := http.NewRequest("GET", url, nil)
	applyBrowserGetHeaders(req.Header)
	resp, err := client.Do(req)
	if err != nil {
		return YandexDocsInfo{}, err
	}
	defer resp.Body.Close()

	htmlBytes, _ := io.ReadAll(resp.Body)
	html := string(htmlBytes)

	utils.Debugf("[YDOCS] fetchDocInfo GET %s -> %d (%d bytes)", url, resp.StatusCode, len(html))

	var cookies []string
	for _, c := range resp.Cookies() {
		cookies = append(cookies, fmt.Sprintf("%s=%s", c.Name, c.Value))
	}

	re := regexp.MustCompile(`<script[^>]*id="client-config"[^>]*>(.*?)</script>`)
	matches := re.FindStringSubmatch(html)
	if len(matches) < 2 {
		// Flag a CAPTCHA/bot-check page explicitly rather than leaving
		// "config not found" to be diagnosed by hand from the HTML preview.
		lower := strings.ToLower(html)
		if strings.Contains(lower, "captcha") {
			utils.Debugf("[YDOCS] response looks like a CAPTCHA/bot-check page, not the doc editor")
		}
		preview := html
		if len(preview) > 2000 {
			preview = preview[:2000]
		}
		utils.Debugf("[YDOCS] HTML preview: %s", preview)
		return YandexDocsInfo{}, fmt.Errorf("config not found")
	}

	var config map[string]interface{}
	if err := json.Unmarshal([]byte(matches[1]), &config); err != nil {
		return YandexDocsInfo{}, fmt.Errorf("parse client-config: %w", err)
	}

	// Every lookup here is a checked type assertion, not config["x"].(T):
	// this runs in a goroutine with no recover(), so an unchecked assertion
	// would crash the process the moment Yandex serves an unexpected page
	// shape (error page, A/B layout, partial response) instead of just
	// failing this connect attempt.
	officeAction, ok := config["officeActionData"].(map[string]interface{})
	if !ok {
		utils.Debugf("[YDOCS] config top-level keys: %v", mapKeys(config))
		return YandexDocsInfo{}, fmt.Errorf("officeActionData missing or malformed")
	}

	editorConfigRaw, ok := officeAction["editor_config"].(map[string]interface{})
	if !ok || editorConfigRaw == nil {
		utils.Debugf("[YDOCS] officeActionData keys: %v", mapKeys(officeAction))
		return YandexDocsInfo{}, fmt.Errorf("editor_config nil - will reconnect")
	}

	balancerURL, ok := officeAction["balancer_url"].(string)
	if !ok {
		// officeActionData + editor_config present but balancer_url missing
		// is the confirmed signature of a newer-generation Yandex document
		// this transport can't talk to; YandexVolgaTransport (a different
		// auth flow - see volga.go's authorize()) can. Not detectable before
		// this point since it's specific to the individual doc, not the URL.
		utils.Debugf("[YDOCS] officeActionData keys: %v, editor_config keys: %v", mapKeys(officeAction), mapKeys(editorConfigRaw))
		return YandexDocsInfo{}, fmt.Errorf("balancer_url missing - this document looks like a newer Yandex Docs type this transport doesn't support; try the Volga transport for this doc_url instead")
	}
	host := strings.TrimPrefix(balancerURL, "https://")

	document, ok := editorConfigRaw["document"].(map[string]interface{})
	if !ok {
		utils.Debugf("[YDOCS] editor_config keys: %v", mapKeys(editorConfigRaw))
		return YandexDocsInfo{}, fmt.Errorf("document missing or malformed")
	}

	token, ok := editorConfigRaw["token"].(string)
	if !ok {
		utils.Debugf("[YDOCS] editor_config keys: %v", mapKeys(editorConfigRaw))
		return YandexDocsInfo{}, fmt.Errorf("editor_config.token missing or malformed")
	}

	docKey, ok := document["key"].(string)
	if !ok {
		utils.Debugf("[YDOCS] document keys: %v", mapKeys(document))
		return YandexDocsInfo{}, fmt.Errorf("document.key missing or malformed")
	}

	perms, _ := document["permissions"].(map[string]interface{})
	if perms == nil {
		perms = make(map[string]interface{})
	}

	return YandexDocsInfo{
		CookieStr:   strings.Join(cookies, "; "),
		Token:       token,
		DocID:       docKey,
		Origin:      balancerURL,
		Host:        host,
		WsURL:       fmt.Sprintf("wss://%s/2024.1.1-375/doc/%s/c/?EIO=4&transport=websocket", host, docKey),
		Permissions: perms,
		OpenCmd: map[string]interface{}{
			"c":      "open",
			"id":     docKey,
			"userid": userID,
			"format": document["fileType"],
			"url":    document["url"],
			"title":  document["title"],
			"lcid":   25,
		},
	}, nil
}

func randUserID() string {
	return fmt.Sprintf("%010d", rand.New(rand.NewSource(time.Now().UnixNano())).Intn(1000000000))
}
