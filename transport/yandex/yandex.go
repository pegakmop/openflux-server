package yandex

import (
	"encoding/base64"
	"encoding/json"
	"fmt"
	"io"
	"math"
	"math/rand"
	"net/http"
	"net/http/cookiejar"
	"regexp"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"github.com/gorilla/websocket"

	"universal-bypass-tool/transport"
	"universal-bypass-tool/utils"
)

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
	return s.Conn.WriteMessage(messageType, data)
}

type YandexDocsTransport struct {
	*transport.BaseTransport

	url        string
	session    *DocSession
	httpClient *http.Client

	userCounter atomic.Int32
	baseUserID  string
}

func NewYandexDocsTransport(url string, config transport.TransportConfig) *YandexDocsTransport {
	jar, _ := cookiejar.New(nil)
	t := &YandexDocsTransport{
		BaseTransport: transport.NewBaseTransport(config),
		url:           url,
		// A fresh, jarless http.Client per fetch (the old behavior) meant
		// every retry looked like a brand-new anonymous visitor - no
		// Set-Cookie response (including on the redirects this request
		// follows) was ever carried into the next request. Yandex appears to
		// use that session-continuity signal when deciding whether to serve
		// the real editable session or a stripped-down view-only page with
		// officeActionData.editor_config == null (see fetchDocInfo) - one
		// persistent client+jar, reused across every reconnect attempt for
		// this transport's lifetime, is what a real browser does too.
		httpClient: &http.Client{
			Jar:           jar,
			CheckRedirect: func(req *http.Request, via []*http.Request) error { return nil },
			Timeout:       30 * time.Second,
			Transport:     &http.Transport{DialContext: transport.ProtectedDialer().DialContext},
		},
	}
	t.baseUserID = randUserID()
	return t
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
		headers.Set("User-Agent", "Mozilla/5.0")
		headers.Set("Origin", info.Origin)
		headers.Set("Cookie", info.CookieStr)
		headers.Set("Host", info.Host)

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

		for t.IsRunning() {
			conn.SetReadDeadline(time.Now().Add(readTimeout))
			_, message, err := conn.ReadMessage()
			if err != nil {
				utils.Debugf("[YDOCS] Read error: %v", err)
				t.SetConnected(false)
				t.scheduleReconnect(attempt, reasonReadError, err)
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
			if err := conn.WriteMessage(websocket.TextMessage, append([]byte("40"), authPkt...)); err != nil {
				return 0, fmt.Errorf("send namespace-connect: %w", err)
			}
			sentConnect = true

		case text == "2":
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
	for t.IsRunning() {
		var packet []byte
		select {
		case packet = <-queue:
		case <-time.After(200 * time.Millisecond):
			// Nothing to send; loop back around just to re-check IsRunning.
			continue
		}

		t.Mu.RLock()
		session := t.session
		t.Mu.RUnlock()
		if session == nil || session.Conn == nil {
			continue
		}

		payload := base64.StdEncoding.EncodeToString(packet)
		msg := fmt.Sprintf(`42["message",{"type":"cursor","cursor":"18;%s"}]`, payload)

		if err := session.safeWrite(websocket.TextMessage, []byte(msg)); err != nil {
			utils.Debugf("[YDOCS] Write error: %v", err)
		}
	}
}

func (t *YandexDocsTransport) keepAliveLoop() {
	ticker := time.NewTicker(t.GetConfig().KeepAliveInterval)
	defer ticker.Stop()
	keepAliveMsg := `42["message",{"type":"cursor","cursor":"18;---KA---"}]`

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

		t.RecordReceive(len(decoded))
		t.CallReceive(decoded)
	}
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
		time.Sleep(delay)
	}
	if !t.IsRunning() {
		return
	}

	t.connectToDoc(attempt + 1)
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
	if cfg.MaxReconnectDelay > 0 && delay > float64(cfg.MaxReconnectDelay) {
		delay = float64(cfg.MaxReconnectDelay)
	}
	return time.Duration(delay)
}

func (t *YandexDocsTransport) fetchDocInfo(url, userID string) (YandexDocsInfo, error) {
	req, _ := http.NewRequest("GET", url, nil)
	req.Header.Set("User-Agent", "Mozilla/5.0")
	resp, err := t.httpClient.Do(req)
	if err != nil {
		return YandexDocsInfo{}, err
	}
	defer resp.Body.Close()

	htmlBytes, _ := io.ReadAll(resp.Body)
	html := string(htmlBytes)

	var cookies []string
	for _, c := range resp.Cookies() {
		cookies = append(cookies, fmt.Sprintf("%s=%s", c.Name, c.Value))
	}

	re := regexp.MustCompile(`<script[^>]*id="client-config"[^>]*>(.*?)</script>`)
	matches := re.FindStringSubmatch(html)
	if len(matches) < 2 {
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
		return YandexDocsInfo{}, fmt.Errorf("officeActionData missing or malformed")
	}

	editorConfigRaw, ok := officeAction["editor_config"].(map[string]interface{})
	if !ok || editorConfigRaw == nil {
		return YandexDocsInfo{}, fmt.Errorf("editor_config nil - will reconnect")
	}

	balancerURL, ok := officeAction["balancer_url"].(string)
	if !ok {
		return YandexDocsInfo{}, fmt.Errorf("balancer_url missing or malformed")
	}
	host := strings.TrimPrefix(balancerURL, "https://")

	document, ok := editorConfigRaw["document"].(map[string]interface{})
	if !ok {
		return YandexDocsInfo{}, fmt.Errorf("document missing or malformed")
	}

	token, ok := editorConfigRaw["token"].(string)
	if !ok {
		return YandexDocsInfo{}, fmt.Errorf("editor_config.token missing or malformed")
	}

	docKey, ok := document["key"].(string)
	if !ok {
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
