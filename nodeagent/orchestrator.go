// Package nodeagent lets an exit-node process serve many keys at once,
// picking up newly-added or newly-disabled keys from the openflux-control
// service instead of being started with one fixed --url per process.
package nodeagent

import (
	"context"
	"fmt"
	"strings"
	"sync"
	"time"

	"universal-bypass-tool/transport"
	"universal-bypass-tool/transport/yandex"
	"universal-bypass-tool/tunnel"
	"universal-bypass-tool/utils"
)

type Config struct {
	ControlURL      string
	NodeToken       string
	PollInterval    time.Duration
	UsageInterval   time.Duration
	HeartbeatPeriod time.Duration
	// ExitMode picks how every worker's TCPTunnel reaches the real
	// internet - see tunnel.ExitMode. Defaults to tunnel.ExitModeRaw
	// (DefaultConfig's zero value), matching every existing managed
	// deployment's current behavior.
	ExitMode tunnel.ExitMode
}

func DefaultConfig(controlURL, nodeToken string) Config {
	return Config{
		ControlURL:      controlURL,
		NodeToken:       nodeToken,
		PollInterval:    20 * time.Second,
		UsageInterval:   20 * time.Second,
		HeartbeatPeriod: 60 * time.Second,
		ExitMode:        tunnel.ExitModeRaw,
	}
}

type worker struct {
	trans   transport.Transport
	tun     *tunnel.TCPTunnel
	docURL  string
	portIdx int

	// lastSent/lastRecv are the transport's cumulative byte counters as of
	// the last usage report, so ReportUsage only sends the delta.
	lastSent uint64
	lastRecv uint64
}

// Orchestrator owns one worker (Transport + TCPTunnel) per active key
// assigned to this node, keeping that set in sync with the control plane.
type Orchestrator struct {
	client *ControlClient
	cfg    Config

	mu      sync.Mutex
	workers map[string]*worker
	ports   portAllocator
}

func NewOrchestrator(cfg Config) *Orchestrator {
	return &Orchestrator{
		client:  NewControlClient(cfg.ControlURL, cfg.NodeToken),
		cfg:     cfg,
		workers: make(map[string]*worker),
	}
}

// portRangeBase/portRangeSize/portRangeMax divide the usable TCP port space
// into fixed-size, non-overlapping blocks - see TCPTunnel.SetPortRange's
// doc comment for why every worker on this node needs one of its own.
// portRangeSize=256 leaves room for a generous number of concurrent
// connections per key while still fitting roughly 250 concurrent workers
// in the space below portRangeMax.
const (
	portRangeBase = 1025
	portRangeSize = 256
	portRangeMax  = 65535
)

// portAllocator hands out disjoint [start, end] port ranges by index,
// recycling an index once its worker stops. Zero value is ready to use.
type portAllocator struct {
	mu   sync.Mutex
	next int
	free []int
}

// alloc reserves the next free range, returning ok=false once the port
// space is exhausted (roughly (portRangeMax-portRangeBase)/portRangeSize
// concurrent workers) - the caller should treat that as "no capacity left
// on this node right now" rather than a fatal error.
func (p *portAllocator) alloc() (idx int, start, end uint16, ok bool) {
	p.mu.Lock()
	defer p.mu.Unlock()

	fromFree := false
	if n := len(p.free); n > 0 {
		idx = p.free[n-1]
		p.free = p.free[:n-1]
		fromFree = true
	} else {
		idx = p.next
		p.next++
	}

	rangeStart := portRangeBase + idx*portRangeSize
	rangeEnd := rangeStart + portRangeSize - 1
	if rangeEnd > portRangeMax {
		// Undo the reservation - this index isn't usable - without
		// disturbing whichever source (free list or the running counter)
		// it actually came from.
		if fromFree {
			p.free = append(p.free, idx)
		} else {
			p.next--
		}
		return 0, 0, 0, false
	}
	return idx, uint16(rangeStart), uint16(rangeEnd), true
}

func (p *portAllocator) release(idx int) {
	p.mu.Lock()
	defer p.mu.Unlock()
	p.free = append(p.free, idx)
}

// Run blocks until ctx is cancelled, driving the poll/usage/heartbeat loops
// and tearing down every worker on exit.
func (o *Orchestrator) Run(ctx context.Context) {
	var wg sync.WaitGroup
	wg.Add(3)

	go func() { defer wg.Done(); o.pollLoop(ctx) }()
	go func() { defer wg.Done(); o.usageLoop(ctx) }()
	go func() { defer wg.Done(); o.heartbeatLoop(ctx) }()

	wg.Wait()

	o.mu.Lock()
	defer o.mu.Unlock()
	for id, w := range o.workers {
		o.stopWorker(w)
		delete(o.workers, id)
	}
}

func (o *Orchestrator) pollLoop(ctx context.Context) {
	o.reconcile(ctx)

	ticker := time.NewTicker(o.cfg.PollInterval)
	defer ticker.Stop()
	for {
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
			o.reconcile(ctx)
		}
	}
}

// workerStartStagger spaces out starting brand-new workers discovered in the
// same reconcile pass - see reconcile's doc comment on why bursting them all
// at once is worth avoiding even though each individual start is cheap and
// non-blocking.
const workerStartStagger = 150 * time.Millisecond

func (o *Orchestrator) reconcile(ctx context.Context) {
	keys, err := o.client.ListKeys(ctx)
	if err != nil {
		utils.Debugf("[NODEAGENT] list keys failed: %v", err)
		return
	}

	active := make(map[string]RemoteKey, len(keys))
	for _, k := range keys {
		active[k.ID] = k
	}

	o.mu.Lock()
	for id, w := range o.workers {
		if _, stillActive := active[id]; !stillActive {
			utils.Debugf("[NODEAGENT] stopping worker for key %s (no longer active)", id)
			o.stopWorker(w)
			delete(o.workers, id)
		}
	}

	var toStart []RemoteKey
	for id, k := range active {
		if _, exists := o.workers[id]; exists {
			continue
		}
		if k.Transport != "yandex" && k.Transport != "yandex_multistream" {
			utils.Debugf("[NODEAGENT] skipping key %s: managed mode only supports the yandex/yandex_multistream transports today", id)
			continue
		}
		if k.Transport == "yandex_multistream" && len(k.DocURLs) < 2 {
			utils.Debugf("[NODEAGENT] skipping key %s: yandex_multistream needs 2+ doc_urls, got %d", id, len(k.DocURLs))
			continue
		}
		toStart = append(toStart, k)
	}
	o.mu.Unlock()

	// Staggered and outside o.mu: starting many keys' WebSocket transports at
	// once (a cold start, or a queued-up control-plane hiccup) would burst
	// dozens of near-simultaneous outbound connections from this one
	// exit-node IP - a pattern that looks like the automated traffic
	// providers' bot detection exists to catch (see fetchDocInfo's CAPTCHA
	// handling). o.mu stays released during the wait so usageLoop/
	// heartbeatLoop keep running against workers already up.
	for i, k := range toStart {
		if i > 0 {
			select {
			case <-ctx.Done():
				return
			case <-time.After(workerStartStagger):
			}
		}

		w, err := o.startWorker(k)
		if err != nil {
			utils.Debugf("[NODEAGENT] failed to start worker for key %s: %v", k.ID, err)
			continue
		}
		utils.Debugf("[NODEAGENT] started worker for key %s", k.ID)

		o.mu.Lock()
		o.workers[k.ID] = w
		o.mu.Unlock()
	}
}

func (o *Orchestrator) startWorker(k RemoteKey) (*worker, error) {
	// Raw mode needs a disjoint port range per worker (see
	// TCPTunnel.SetPortRange) since all raw-mode workers share one real
	// IP/raw socket. Proxy mode's egress is a plain net.Dial with its own
	// OS ephemeral port allocation, so it skips this reservation entirely -
	// otherwise portRangeSize/portRangeMax would cap even proxy mode at
	// ~252 concurrent keys for no reason. portIdx stays -1 to mark "no
	// reservation to release".
	portIdx := -1
	var portStart, portEnd uint16
	if o.cfg.ExitMode == tunnel.ExitModeRaw {
		var ok bool
		portIdx, portStart, portEnd, ok = o.ports.alloc()
		if !ok {
			return nil, fmt.Errorf("no port range capacity left on this node")
		}
	}

	// e2e_encryption is binding, not advisory (see
	// transport.EncryptedTransport's doc comment on the auto-detect
	// leniency this replaced) - if it's on for this key but there's no
	// token to derive a matching key from, refuse to start the worker
	// rather than silently running it unencrypted, which would defeat the
	// whole point of the setting.
	if k.E2EEncryption && k.Token == "" {
		if portIdx >= 0 {
			o.ports.release(portIdx)
		}
		return nil, fmt.Errorf("key %s has e2e_encryption on but no usable token", k.ID)
	}

	// For yandex_multistream, each stream must compress/encrypt itself
	// before MultiStreamTransport ever sees the data: its Send() reads
	// streamIndex off what it assumes is a raw IP/TCP header (see
	// multistream.go) to pin one real connection to one stream - handed
	// compressed/encrypted bytes instead, that read is just noise. Mirrors
	// mobile.go's client-side wrapYandex.
	wrapStream := func(yd *yandex.YandexDocsTransport, idx int, perStreamKey bool) transport.Transport {
		if !k.E2EEncryption {
			yd.EnableSelfCompression()
			return yd
		}
		if perStreamKey {
			yd.EnableEncryptedSelfCompressionForStream(k.Token, true, idx)
		} else {
			yd.EnableEncryptedSelfCompression(k.Token, true)
		}
		return yd
	}

	var trans transport.Transport
	label := k.DocURL
	if k.Transport == "yandex_multistream" {
		streams := make([]transport.Transport, len(k.DocURLs))
		for i, url := range k.DocURLs {
			streams[i] = wrapStream(yandex.NewYandexDocsTransport(url, transport.DefaultConfig()), i, true)
		}
		trans = transport.NewMultiStreamTransport(streams)
		label = strings.Join(k.DocURLs, ",")
	} else {
		trans = wrapStream(yandex.NewYandexDocsTransport(k.DocURL, transport.DefaultConfig()), 0, false)
	}
	if err := trans.Start(); err != nil {
		if portIdx >= 0 {
			o.ports.release(portIdx)
		}
		return nil, err
	}
	tun := tunnel.NewTCPTunnelMode(trans, true, o.cfg.ExitMode)
	if portIdx >= 0 {
		tun.SetPortRange(portStart, portEnd)
	}
	return &worker{trans: trans, tun: tun, docURL: label, portIdx: portIdx}, nil
}

func (o *Orchestrator) stopWorker(w *worker) {
	w.trans.Stop()
	w.tun.Close()
	if w.portIdx >= 0 {
		o.ports.release(w.portIdx)
	}
}

func (o *Orchestrator) usageLoop(ctx context.Context) {
	ticker := time.NewTicker(o.cfg.UsageInterval)
	defer ticker.Stop()
	for {
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
			o.reportUsage(ctx)
		}
	}
}

func (o *Orchestrator) reportUsage(ctx context.Context) {
	o.mu.Lock()
	deltas := make([]UsageDelta, 0, len(o.workers))
	for id, w := range o.workers {
		stats := w.trans.Stats()
		sentDelta := diffCounter(w.lastSent, stats.BytesSent)
		recvDelta := diffCounter(w.lastRecv, stats.BytesReceived)
		w.lastSent = stats.BytesSent
		w.lastRecv = stats.BytesReceived

		if sentDelta == 0 && recvDelta == 0 {
			continue
		}
		deltas = append(deltas, UsageDelta{
			KeyID:              id,
			BytesSentDelta:     int64(sentDelta),
			BytesReceivedDelta: int64(recvDelta),
		})
	}
	o.mu.Unlock()

	if len(deltas) == 0 {
		return
	}

	// The control plane rejects a request over 1000 deltas outright (see
	// handleNodeUsage's own cap) rather than partially applying it - above
	// that many concurrently active keys on one node, sending them all in
	// one request silently drops the ENTIRE usage report every tick instead
	// of just the excess. Chunking keeps usage/quota enforcement correct
	// past that scale instead of only below it.
	const usageChunkSize = 1000
	var disabledNow []string
	for start := 0; start < len(deltas); start += usageChunkSize {
		end := start + usageChunkSize
		if end > len(deltas) {
			end = len(deltas)
		}
		chunkDisabled, err := o.client.ReportUsage(ctx, deltas[start:end])
		if err != nil {
			utils.Debugf("[NODEAGENT] report usage failed: %v", err)
			continue
		}
		disabledNow = append(disabledNow, chunkDisabled...)
	}

	if len(disabledNow) == 0 {
		return
	}

	o.mu.Lock()
	defer o.mu.Unlock()
	for _, id := range disabledNow {
		if w, ok := o.workers[id]; ok {
			utils.Debugf("[NODEAGENT] key %s went over quota, stopping worker", id)
			o.stopWorker(w)
			delete(o.workers, id)
		}
	}
}

// diffCounter handles the (rare) case of a transport reconnect resetting its
// cumulative counters: a negative-looking diff is treated as "count from
// zero again" rather than underflowing a uint64 subtraction.
func diffCounter(previous, current uint64) uint64 {
	if current < previous {
		return current
	}
	return current - previous
}

func (o *Orchestrator) heartbeatLoop(ctx context.Context) {
	ticker := time.NewTicker(o.cfg.HeartbeatPeriod)
	defer ticker.Stop()
	for {
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
			if err := o.client.Heartbeat(ctx); err != nil {
				utils.Debugf("[NODEAGENT] heartbeat failed: %v", err)
			}
		}
	}
}
