// Package nodeagent lets an exit-node process serve many keys at once, picking up newly added/disabled keys from openflux-control instead of one fixed --url per process.
package nodeagent

import (
	"context"
	"fmt"
	"log"
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
	ExitMode        tunnel.ExitMode
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

	lastSent uint64
	lastRecv uint64
}

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

// portRangeSize trades against portAllocator's ceiling on concurrent workers (~2015 at 32); raise only alongside lowering nodes.max_keys, since a range can't be resized once its worker is running.
const (
	portRangeBase = 1025
	portRangeSize = 32
	portRangeMax  = 65535
)

type portAllocator struct {
	mu   sync.Mutex
	next int
	free []int
}

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

	// Worker starts are staggered outside o.mu so a cold start doesn't burst dozens of near-simultaneous connections from one exit-node IP, which looks like bot traffic to the doc providers.
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
			// Port-range exhaustion is logged unconditionally (not gated behind --debug) since it otherwise fails silently, the same way, every poll cycle.
			if o.cfg.ExitMode == tunnel.ExitModeRaw && strings.Contains(err.Error(), "no port range capacity") {
				log.Printf("[NODEAGENT] key %s not started: %v - this node has reached its concurrent-key ceiling (see portRangeSize in orchestrator.go)", k.ID, err)
			} else {
				utils.Debugf("[NODEAGENT] failed to start worker for key %s: %v", k.ID, err)
			}
			continue
		}
		utils.Debugf("[NODEAGENT] started worker for key %s", k.ID)

		o.mu.Lock()
		o.workers[k.ID] = w
		o.mu.Unlock()
	}
}

func (o *Orchestrator) startWorker(k RemoteKey) (*worker, error) {
	// Raw mode needs a disjoint port range per worker (see TCPTunnel.SetPortRange); proxy mode's plain net.Dial needs no such reservation.
	portIdx := -1
	var portStart, portEnd uint16
	if o.cfg.ExitMode == tunnel.ExitModeRaw {
		var ok bool
		portIdx, portStart, portEnd, ok = o.ports.alloc()
		if !ok {
			return nil, fmt.Errorf("no port range capacity left on this node")
		}
	}

	// e2e_encryption is binding: if it's on for a key but there's no token to derive a matching key, refuse to start the worker rather than silently running unencrypted.
	if k.E2EEncryption && k.Token == "" {
		if portIdx >= 0 {
			o.ports.release(portIdx)
		}
		return nil, fmt.Errorf("key %s has e2e_encryption on but no usable token", k.ID)
	}

	// For yandex_multistream, each stream must compress/encrypt itself before MultiStreamTransport sees the data, since Send() reads streamIndex off what it assumes is a raw header.
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

	// The control plane rejects a usage request over 1000 deltas outright rather than partially applying it, so this chunks reports to keep enforcement correct past that scale.
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

// diffCounter treats a negative-looking diff (from a transport reconnect resetting counters) as "count from zero again" rather than underflowing a uint64 subtraction.
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
