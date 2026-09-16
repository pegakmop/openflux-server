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

// Sending one WebSocket frame per queued packet was cheap enough for a
// single mobile client but scales badly on an exit node juggling many keys
// at once - every packet pays its own base64 encode, JSON string format,
// and WriteMessage syscall regardless of size, and the OS/GC overhead of
// that per-message cost is what actually dominates at higher concurrent
// packet rates, not the bytes themselves. writerLoop below batches several
// queued packets into one length-prefixed blob (the same framing Volga
// already uses - see decodeBatch in volga.go, reused here as-is) before
// base64-encoding and sending it as a single "cursor" message, the same way
// Volga batches multiple packets into one relay HTTP POST.
//
// batchMarker prefixes a batched payload's first byte so a peer can tell it
// apart from a lone compressed packet: compress() only ever emits 0x00
// (stored) or 0x1F (LZ4) as its own first byte, so 0xFE never collides with
// a real single-packet payload. That only covers RECEIVING from a mixed-
// version peer, though - see kaBatchCapabilityToken for why sending is
// gated separately.
const batchMarker = 0xFE

// kaBatchCapabilityToken rides inside the keepalive to tell a peer "my code
// understands batchMarker" before ever sending it one. Without this, a
// build that unconditionally batches breaks a not-yet-updated peer in
// EITHER role - its own receive path has no batchMarker check at all, so a
// batched frame just fails to decompress. Peer capability, not which role
// this transport plays (client vs exit node), is what writerLoop's flush
// gates sending on - see peerBatches.
const kaBatchCapabilityToken = "+batch1"

// zstdBatchMarker flags a payload as EnableSelfCompression's whole-batch
// format: raw (not yet per-packet-compressed) packets, length-prefixed and
// zstd-compressed as one unit via transport.EncodeBatch - see
// YandexDocsTransport.EnableSelfCompression's doc comment for why this beats
// batchMarker's "batch of individually-LZ4'd packets" for the non-encrypted
// case. Distinct from batchMarker (0xFE) and from the 0x00/0x1F a lone
// transport.Compress'd packet's own first byte can be, so a peer that
// understands one format never mistakes a frame in the other for its own -
// see kaZstdBatchCapabilityToken for why sending is still gated separately
// from merely picking a marker byte that doesn't collide.
const zstdBatchMarker = 0xFD

// kaZstdBatchCapabilityToken is kaBatchCapabilityToken's counterpart for
// zstdBatchMarker - a peer's keepalive must carry this before this transport
// ever sends it a zstd-whole-batch frame, exactly the same bootstrapping
// rule batchMarker already established (see peerBatches). Appended after
// kaBatchCapabilityToken in the same keepalive message; a peer that only
// recognizes the older token (or neither) still matches "---KA---" and
// "+batch1" as an exact substring each and ignores whatever trails after,
// same as always.
const kaZstdBatchCapabilityToken = "+zbatch1"

// kaEncSelfCompressToken is kaZstdBatchCapabilityToken's counterpart for
// EnableEncryptedSelfCompression specifically - see that method's doc
// comment for why understanding zstdBatchMarker in general (peerZstdBatches)
// isn't enough to prove a peer also does encrypted self-compression for a
// given e2e_encryption key: sending this token is conditional on this
// transport instance actually being in that mode (see keepAliveLoop),
// unlike kaBatchCapabilityToken/kaZstdBatchCapabilityToken, which every
// instance sends unconditionally regardless of what it uses them for.
// Appended after the other two tokens in the same keepalive; ignored the
// same way by any peer that doesn't recognize it.
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

	// recentSent guards against processing our own data. Yandex's doc
	// broadcasts every "cursor" event to every participant in the
	// document, sender included - the same self-echo a collaborative
	// editor's cursor broadcast normally is. Nothing here previously
	// checked authorship before decoding a "cursor" message and handing it
	// to CallReceive, so a tunnel packet we ourselves just sent (see
	// writerLoop) came right back over the same socket and got reinjected
	// as if the peer had sent it - a real packet, correctly formed, just
	// flowing in a direction gvisor's NAT/forwarding never expects on that
	// NIC (that's what "unexpected transport protocol = 0" turned out to
	// be a symptom of, not a cause). Recording a short-lived hash of every
	// payload we send and skipping any inbound payload that matches lets
	// this be caught without needing to know Yandex's exact broadcast
	// wrapping format, and without touching the wire format the real
	// backend expects.
	recentSentMu sync.Mutex
	recentSent   map[uint32]time.Time

	// peerBatches is learned from the peer's own keepalive (see
	// kaBatchCapabilityToken) - only once we know the peer's code
	// recognizes batchMarker do we send it batched frames, so a build
	// running against a not-yet-updated peer (or vice versa) keeps using
	// the legacy one-packet-per-message format instead of sending
	// something the other side can't parse.
	peerBatches atomic.Bool

	// peerZstdBatches is peerBatches' counterpart for zstdBatchMarker - see
	// kaZstdBatchCapabilityToken.
	peerZstdBatches atomic.Bool

	// selfCompress is EnableSelfCompression's/EnableEncryptedSelfCompression's
	// flag - set at most once, before Start, never written again afterward
	// (same contract as SetEventCallback's), so reading it from
	// writerLoop/handleMessage's goroutines without a lock is safe. Same for
	// encrypted/encSend/encRecv below.
	selfCompress bool

	// encrypted, encSend, encRecv, encSendCtr are
	// EnableEncryptedSelfCompression's state - encrypted is false unless that
	// (not the plain EnableSelfCompression) was called; encSend/encRecv are
	// this connection's derived ChaCha20-Poly1305 keys (see
	// transport.DeriveDirectionalKeys), and encSendCtr is this side's own
	// nonce counter, independent of any external transport.EncryptedTransport
	// instance (there is none in this mode).
	encrypted  bool
	encSend    [chacha20poly1305.KeySize]byte
	encRecv    [chacha20poly1305.KeySize]byte
	encSendCtr atomic.Uint64

	// peerEncSelfCompress is peerZstdBatches' counterpart specifically for
	// EnableEncryptedSelfCompression - see kaEncSelfCompressToken's doc
	// comment for why peerZstdBatches alone isn't enough proof for the
	// encrypted case.
	peerEncSelfCompress atomic.Bool

	// wakeReconnect, guarded by Mu, is non-nil exactly while scheduleReconnect
	// is sleeping out a backoff delay between attempts - see ForceReconnect.
	wakeReconnect chan struct{}
}

// EnableSelfCompression switches this transport into managing its own
// per-batch compression (see writerLoop/handleMessage) instead of expecting
// a caller-side transport.CompressedTransport to compress each packet
// before Send ever sees it. Call before Start; changing this after is not
// safe (same contract as SetEventCallback).
//
// Why this exists: transport.CompressedTransport compresses one packet at a
// time, before writerLoop ever groups packets into a batch - LZ4 on each
// packet in isolation misses the redundancy between packets in the same
// batch (repeated headers, similar payloads) that compressing the whole
// batch at once would catch, and pays a full LZ4 frame's overhead per
// packet instead of once per batch. Once a peer's keepalive proves it
// understands zstdBatchMarker (see kaZstdBatchCapabilityToken), writerLoop
// switches to framing a whole batch of RAW packets and zstd-compressing
// that as one unit (transport.EncodeBatch) - strictly more efficient, never
// less, than compressing each packet alone. Until then (or against a peer
// that never upgrades) it reproduces transport.CompressedTransport's exact
// per-packet format itself, so wire compatibility with an unmodified peer
// is unaffected either way.
//
// NOT used together with transport.CompressedTransport: a caller enabling
// this must not also wrap this transport in CompressedTransport, or packets
// get compressed twice on send and the receive side (which now decompresses
// internally - see handleMessage) would be handed already-decompressed
// bytes a second time. For a key with end-to-end encryption on, use
// EnableEncryptedSelfCompression instead - not this plus a separate
// transport.EncryptedTransport wrapper, which would encrypt this method's
// own batching decisions no differently than any other opaque bytes and
// lose the whole point (see that method's doc comment). See
// mobile.buildTransport and nodeagent.Orchestrator.startWorker for the
// actual call sites and how they pick between the two.
func (t *YandexDocsTransport) EnableSelfCompression() {
	t.selfCompress = true
}

// EnableEncryptedSelfCompression is EnableSelfCompression plus end-to-end
// encryption, for a key with e2e_encryption on. token/isExitNode derive the
// same keys transport.NewEncryptedTransport would (see
// transport.DeriveDirectionalKeys) - both ends must call this with the same
// token and opposite isExitNode. Call before Start; changing this after is
// not safe (same contract as EnableSelfCompression).
//
// Why a whole-batch encrypted format needs its own method rather than just
// wrapping EnableSelfCompression's output in transport.EncryptedTransport
// externally: encrypting each packet before batching (what
// transport.CompressedTransport(transport.EncryptedTransport(...)) does
// today) destroys exactly the cross-packet redundancy batching whole raw
// packets before compressing was meant to exploit - ChaCha20-Poly1305
// ciphertext is high-entropy by design, so zstd would find nothing to
// compress in a batch of already-encrypted packets. This method instead
// batches and zstd-compresses RAW packets first (transport.EncodeBatch, same
// as EnableSelfCompression) and encrypts the WHOLE compressed batch as one
// unit (transport.Seal) - encryption still only ever seals already-compressed
// bytes, same invariant transport.EncryptedTransport's doc comment
// describes, just applied to a batch instead of one packet.
//
// Compatibility works the same way as EnableSelfCompression, with one added
// wrinkle: understanding zstdBatchMarker in general (peerZstdBatches) does
// NOT prove a peer also does ENCRYPTED self-compression for this specific
// key - a peer can easily be running code new enough for one but still using
// the traditional transport.CompressedTransport(transport.EncryptedTransport(...))
// wrapping for an e2e_encryption key (that's what happens without this
// method), so this is gated by its own separate capability token,
// kaEncSelfCompressToken, sent only by an instance actually in this mode.
// Until a peer proves it (or if it never does), this reproduces the
// traditional wrapping's EXACT wire format itself: encrypt each
// transport.Compress'd packet independently (transport.Seal on the
// compressed bytes, matching transport.EncryptedTransport.Send byte for
// byte) before batching those ciphertexts the old way - so an unmodified
// peer using the external-wrapper architecture for this same key can decrypt
// and decompress it with no changes on its end, exactly like
// EnableSelfCompression's own fallback. Receiving is unconditional either
// way (see handleMessage): this side's own key derivation lets it decrypt
// either format from any peer that shares the same token, whether or not
// that peer is itself using this method.
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

// normalizeDocURL rewrites a Yandex Disk share link into the equivalent
// Yandex Docs URL fetchDocInfo actually knows how to fetch. A disk.yandex.ru
// "/i/<hash>" share link and the docs.yandex.ru edit link serve the same
// client-config-bearing page for a supported document, just under
// different hostnames - a plain host swap is all that's needed, path and
// query string carry over untouched. Used on both the client and the
// exit-node side, since both run this same transport - one normalization
// site covers whichever end of the tunnel a user pastes a disk.yandex.ru
// link into. Anything else (a different host, a malformed URL, a
// disk.yandex.ru path that isn't a share link) passes through unchanged
// and is left for the actual HTTP fetch to accept or reject.
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

// Send queues data for the writer loop to actually put on the wire.
// Deliberately does not require IsConnected(): a session's WriteQueue is
// reused across a reconnect (see connectToDoc) precisely so a brief drop
// doesn't have to lose data, but an early return here for "not connected
// right now" was throwing every packet away for the entire reconnect
// window regardless - the queue existed but nothing during a drop ever
// reached it. A connection blip that would otherwise have been invisible
// (queued, then drained once the new session comes up) was instead forcing
// the real end-to-end TCP connection several hops away to notice the loss
// and retransmit on its own, much slower, timeout.
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
		// A WebSocket upgrade, not a page load - Sec-Fetch-Dest/Mode differ
		// from applyBrowserGetHeaders' document-navigation values
		// accordingly (real Firefox sends these for a same-origin WS
		// connection opened from a page it just loaded).
		headers.Set("Sec-Fetch-Dest", "websocket")
		headers.Set("Sec-Fetch-Mode", "websocket")
		headers.Set("Sec-Fetch-Site", "same-origin")

		conn, _, err := dialer.Dial(info.WsURL, headers)
		if err != nil {
			utils.Debugf("[YDOCS] WebSocket dial failed: %v", err)
			t.scheduleReconnect(attempt, reasonDialFailed, err)
			return
		}

		// The engine.io/socket.io handshake must complete (open packet,
		// then our namespace-connect, then the server's connect ack)
		// before anything else goes over this socket. Sending the auth
		// event packet (or, worse, real tunneled data once writerLoop
		// starts draining the queue) ahead of that ack lands it in a
		// namespace the server hasn't confirmed yet, which is exactly what
		// was making OnlyOffice's backend tear the connection down with
		// close code 1005 in a loop.
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
				// A session that stayed up for a while dropping is a normal,
				// unremarkable blip (Yandex's own infra recycling the
				// connection, a brief network hiccup) - not evidence the
				// backend or network is struggling and reconnects should
				// slow down for. Without this, attempt only ever grows
				// across a long-lived transport's whole life, so backoff
				// eventually settles at MaxReconnectDelay and stays there
				// for every future reconnect, even hours later when nothing
				// is actually wrong - a working connection ends up waiting
				// up to 30s to come back after every routine drop.
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
//     server's actual pingInterval/pingTimeout;
//  2. client -> server: socket.io namespace-connect ("40{"token":...}"),
//     sent only once (1) has arrived;
//  3. server -> client: namespace-connect ack ("40{"sid":...}") or a
//     connect-error ("44...") - only once the ack arrives is this socket
//     actually usable for anything else.
//
// Engine.io pings ("2") can arrive at any point in this sequence and are
// answered ("3") immediately regardless of handshake progress, same as in
// steady-state. It returns the read-idle timeout to apply for the rest of
// this connection's life (derived from the server's own ping settings so a
// silently-dead connection is detected instead of blocking forever).
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
			// batch holds RAW packets - frame+zstd-compress the whole thing
			// as one unit, then encrypt THAT as one unit - see
			// EnableEncryptedSelfCompression.
			t.sendEncryptedZstdBatch(session, batch)
		case t.encrypted:
			// Peer hasn't (yet, or ever) proven it does encrypted
			// self-compression for this key - reproduce
			// transport.CompressedTransport(transport.EncryptedTransport(...))'s
			// exact per-packet format ourselves: compress, then encrypt,
			// each packet independently, before falling back to the
			// existing legacy batch/single path with the resulting
			// ciphertexts - so an unmodified peer using that external
			// wrapping for this same key can decrypt and decompress it
			// with no changes on its end.
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
			// batch holds RAW packets in self-compress mode (nothing
			// upstream compressed them - see EnableSelfCompression) - frame
			// and zstd-compress the whole thing as one unit.
			t.sendZstdBatch(session, batch)
		case t.selfCompress:
			// Peer hasn't (yet, or ever) proven it understands
			// zstdBatchMarker - reproduce transport.CompressedTransport's
			// exact per-packet format ourselves before falling back to the
			// existing legacy batch/single path, so the bytes on the wire
			// are identical to what an unmodified peer already expects.
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
		// keeps pointing at the old, now-dead session until a new one
		// replaces it, so checking session/session.Conn for nil here never
		// actually catches a drop. Without also checking IsConnected(),
		// this dequeued a packet from the queue - the one piece of state
		// Send's fix relies on to survive a reconnect - and then threw it
		// away on the write to that dead connection anyway, every single
		// time. Waiting for IsConnected() before ever touching the channel
		// is what actually keeps queued data queued until a live session
		// exists to drain it into - batch (if anything is held) waits here
		// right along with it, for the same reason.
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
// own [len,data]... encoding, reused verbatim via decodeBatch on the
// receiving end), base64s it, and writes it as a single "cursor" message -
// one WriteMessage syscall and one self-echo hash for however many packets
// batch holds, instead of one of each per packet.
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
		// batch's items are already-processed, opaque-to-this-function bytes
		// by the time they get here - compressed (by an external
		// transport.CompressedTransport, or by writerLoop's own self-compress
		// fallback), and for an encrypted transport's fallback, sealed on top
		// of that too - never raw IP packets, so parsing them here would
		// print convincing-looking nonsense instead of failing loudly.
		// Byte/packet counts are the only things safe to claim at this layer.
		utils.Debugf("[YDOCS] -> %d bytes (%d packets)\n", len(framed), len(batch))
	}

	payload := base64.StdEncoding.EncodeToString(framed)
	msg := fmt.Sprintf(`42["message",{"type":"cursor","cursor":"18;%s"}]`, payload)

	if err := session.safeWrite(websocket.TextMessage, []byte(msg)); err != nil {
		utils.Debugf("[YDOCS] Write error: %v", err)
	}
}

// sendZstdBatch frames batch (RAW packets - see EnableSelfCompression) with
// zstdBatchMarker + transport.EncodeBatch's whole-batch zstd encoding,
// base64s it, and writes it as a single "cursor" message. Only called once
// the peer's keepalive has proven it understands zstdBatchMarker - see
// kaZstdBatchCapabilityToken.
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

// sendEncryptedZstdBatch is sendZstdBatch plus encryption: the same
// [zstdBatchMarker][transport.EncodeBatch(batch)] plaintext, sealed as one
// unit with transport.Seal before going on the wire - see
// EnableEncryptedSelfCompression's doc comment. Only called once the peer's
// keepalive has proven it understands this format - see
// kaEncSelfCompressToken.
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

// sendSingle is sendBatch without the batchMarker framing - the exact
// one-packet-per-message format from before batching existed, used until
// the peer's keepalive proves it understands batched frames (see
// peerBatches).
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

// markSent records that data was just sent, so a later self-echo of it
// arriving back through handleMessage can be recognized and dropped - see
// YandexDocsTransport.recentSent's doc comment. Entries expire on their own
// (checked in wasRecentlySent) rather than needing an explicit size cap: an
// echo either arrives within a couple of seconds or not at all, so nothing
// legitimate is lost by letting old entries age out during opportunistic
// cleanup here.
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
// within the last 5 seconds - a real echo of our own traffic always arrives
// within one round trip to Yandex's servers, far under that window, while an
// unrelated packet from the peer coincidentally producing the same CRC32 is
// astronomically unlikely.
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
	// All trailing tokens ride inside the existing "---KA---" keepalive
	// (still an exact substring each, so a peer understanding only some - or
	// none - still matches "---KA---" and whichever token(s) it knows, and
	// ignores the rest same as always) - see
	// peerBatches/peerZstdBatches/peerEncSelfCompress and
	// kaBatchCapabilityToken/kaZstdBatchCapabilityToken/kaEncSelfCompressToken.
	// Unlike the other two (sent unconditionally by every instance),
	// kaEncSelfCompressToken is conditional on t.encrypted - see that
	// constant's doc comment on why it has to specifically prove THIS
	// instance is in encrypted self-compression mode, not just that its code
	// understands the format in general.
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
				// Force the blocked ReadMessage() in this session's read
				// loop to return immediately instead of waiting out the
				// full read-deadline window, so reconnection starts right
				// away rather than up to defaultPingWindow later.
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

	// Socket.IO ping - respond with pong (use safeWrite)
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

		// The doc broadcasts every cursor event to every participant,
		// sender included - without this check our own just-sent packet
		// comes back here as if the peer had sent it. See recentSent's
		// doc comment on YandexDocsTransport for why this bit us badly:
		// it isn't a rare glitch, it's every single packet we send.
		if t.wasRecentlySent(decoded) {
			if utils.IsVerbose() {
				utils.Debugf("[YDOCS] dropped self-echo (%d bytes)\n", len(decoded))
			}
			return
		}

		if utils.IsVerbose() {
			// Same caveat as writerLoop's "-> N bytes" line: decoded is
			// still pre-decompression at this layer, not a raw IP packet.
			utils.Debugf("[YDOCS] <- %d bytes\n", len(decoded))
		}

		t.RecordReceive(len(decoded))

		if t.encrypted {
			t.handleEncryptedMessage(decoded)
			return
		}

		// zstdBatchMarker (see sendZstdBatch) flags decoded as
		// EnableSelfCompression's whole-batch format: RAW packets, framed
		// and zstd-compressed as one unit. Only ever sent to a peer that
		// already proved (via keepalive) it understands this, but decoding
		// it doesn't depend on our OWN t.selfCompress - a peer capable of
		// sending it is, by construction, also capable of receiving the
		// plain packets this yields, so handing them straight to
		// CallReceive is correct regardless of which mode this side itself
		// is in.
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

		// batchMarker (see sendBatch) flags decoded as several
		// length-prefixed packets rather than one lone payload - the
		// framing a not-yet-updated peer's packets never carry
		// (transport.Compress only ever emits 0x00/0x1F as its own first
		// byte), so this stays correct talking to either version of this
		// transport. Each item here is still in transport.Compress's
		// per-packet format either way (that's what sendBatch always
		// batches, self-compress mode included - see writerLoop's flush);
		// self-compress mode has no external transport.CompressedTransport
		// to undo that for it, so it must do so itself before handing raw
		// packets upward - a non-self-compress transport must NOT, since
		// its own external CompressedTransport.Receive still expects to do
		// that decompression itself, same as always.
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

// handleEncryptedMessage is handleMessage's dispatch once decoded (still
// base64-decoded wire bytes, self-echo already ruled out) is known to belong
// to an EnableEncryptedSelfCompression'd transport - every path here ends in
// a decrypt, but AT A DIFFERENT POINT depending on which of the three wire
// formats EnableEncryptedSelfCompression's doc comment describes actually
// arrived:
//
//   - the legacy per-item fallback (writerLoop's "t.encrypted" branch when
//     the peer hasn't confirmed kaEncSelfCompressToken) has an UNENCRYPTED,
//     structurally-visible batchMarker wrapping individually encrypted
//     items - each has to be decrypted (and then decompressed) separately,
//     the same as an unmodified peer's own external
//     transport.EncryptedTransport would for each one;
//   - the new whole-batch format (sendEncryptedZstdBatch) and the old
//     single-item fallback both have NO visible structure at all - the
//     entire blob is ciphertext, and only decrypting it reveals which of
//     the two it is (zstdBatchMarker as the plaintext's first byte, or not).
//
// Decrypting the wrong span (e.g. attempting to decrypt the whole
// batchMarker-wrapped blob as one unit) would just fail authentication and
// drop everything - this dispatch exists specifically so that never happens.
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

// scheduleReconnect waits out an exponential backoff (see
// transport.DefaultConfig's ReconnectDelay/ReconnectMultiplier/
// MaxReconnectDelay) before retrying, instead of hammering the server in a
// tight loop every time a connection attempt fails fast. cause is the
// actual error that triggered this retry - reasonCode alone only tells a
// human-facing log which of a handful of fixed categories it falls into
// ("couldn't reach the document"), not what specifically went wrong; cause
// is carried in the same event for a caller that wants to show both.
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
		// concurrent ForceReconnect either sees it (and closes it, ending
		// the select below immediately) or arrives too early/late to matter
		// (nothing to interrupt in either case, same as before this field
		// existed).
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
// connection so its read loop notices and redials through the usual
// scheduleReconnect path, or, if no connection is up and it's instead
// sleeping out a backoff delay between attempts, cuts that wait short. A
// no-op if neither applies (not started yet, or already mid-attempt past
// the wait).
//
// Why this exists at all: a network change (Wi-Fi to mobile data, or back)
// often leaves the old socket silently dead rather than reset - nothing
// tells this transport's read loop the connection is gone until a read
// finally times out, which can take far longer than the backoff delay this
// skips. A caller that already knows the network changed (a mobile OS
// callback, an AntiNet-style host event) can report that here instead of
// waiting for TCP to notice on its own.
func (t *YandexDocsTransport) ForceReconnect() {
	t.Mu.Lock()
	session := t.session
	// t.session is never nil'd on disconnect (see connectToDoc/writerLoop's
	// comment on the same fact) - it keeps pointing at the last session,
	// live or not, so session != nil alone can't tell "connected right now"
	// apart from "sleeping out a backoff with a stale session left over".
	// IsConnected() is the field that actually tracks that distinction.
	live := t.IsConnected()
	wake := t.wakeReconnect
	t.wakeReconnect = nil // claimed here, under the same lock, so a second concurrent call can't double-close wake below
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
	// +0-50% jitter, applied before the cap so MaxReconnectDelay stays a
	// true ceiling: every reconnect dials a brand new WebSocket, which
	// Yandex's own doc-collab backend registers as a brand new participant
	// in the room regardless of client-side user-id reuse (ported from
	// upstream p1neappleXpress/OpenFlux, which found this live - "ghost"
	// participants piling up across a failure streak, logged from the
	// server's own participant-list messages). Without jitter, many clients
	// losing the same document at once (a network-wide blip, an exit-node
	// restart) would retry in lockstep at identical delays instead of
	// spreading out.
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
		// Without this, "config not found" was a dead end - no way to tell
		// a CAPTCHA page apart from a login redirect, a maintenance page,
		// or something else entirely without reproducing it by hand.
		// SmartCaptcha/showcaptcha/checkbox-captcha are the markers Yandex's
		// own bot-check pages actually use, so this is flagged explicitly
		// rather than left for someone reading the preview to notice.
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

	// Every lookup below used to be an unchecked type assertion
	// (config["x"].(T)), which panics - and since this runs in a goroutine
	// with no recover(), crashes the entire process - the moment Yandex
	// serves a page shaped even slightly differently than expected (an
	// error/maintenance page, an A/B-tested layout, a partly-loaded
	// response). All of it is now checked and turned into a plain error
	// that triggers a reconnect instead.
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
		// Confirmed in production: this exact signature (officeActionData +
		// editor_config both present, only balancer_url missing - not a
		// captcha or error page, which would fail the "config not found"
		// check above instead) is what a newer-generation Yandex document
		// looks like to this transport. This transport (the classic
		// engine.io/socket.io editor session docs.yandex.ru serves) doesn't
		// know how to talk to those; YandexVolgaTransport does (a different
		// auth flow entirely - see volga.go's authorize()). There's no way
		// to tell a document is this type before hitting this error - it's
		// specific to the individual doc, not the URL shape - so the best
		// this can do is name the actual fix instead of a bare parse error.
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
