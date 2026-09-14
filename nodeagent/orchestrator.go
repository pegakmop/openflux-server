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
}

func DefaultConfig(controlURL, nodeToken string) Config {
	return Config{
		ControlURL:      controlURL,
		NodeToken:       nodeToken,
		PollInterval:    20 * time.Second,
		UsageInterval:   20 * time.Second,
		HeartbeatPeriod: 60 * time.Second,
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
	defer o.mu.Unlock()

	for id, w := range o.workers {
		if _, stillActive := active[id]; !stillActive {
			utils.Debugf("[NODEAGENT] stopping worker for key %s (no longer active)", id)
			o.stopWorker(w)
			delete(o.workers, id)
		}
	}

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

		w, err := o.startWorker(k)
		if err != nil {
			utils.Debugf("[NODEAGENT] failed to start worker for key %s: %v", id, err)
			continue
		}
		utils.Debugf("[NODEAGENT] started worker for key %s", id)
		o.workers[id] = w
	}
}

func (o *Orchestrator) startWorker(k RemoteKey) (*worker, error) {
	portIdx, portStart, portEnd, ok := o.ports.alloc()
	if !ok {
		return nil, fmt.Errorf("no port range capacity left on this node")
	}

	var trans transport.Transport
	label := k.DocURL
	if k.Transport == "yandex_multistream" {
		streams := make([]transport.Transport, len(k.DocURLs))
		for i, url := range k.DocURLs {
			streams[i] = yandex.NewYandexDocsTransport(url, transport.DefaultConfig())
		}
		trans = transport.NewMultiStreamTransport(streams)
		label = strings.Join(k.DocURLs, ",")
	} else {
		trans = yandex.NewYandexDocsTransport(k.DocURL, transport.DefaultConfig())
	}
	if k.Token != "" {
		trans = transport.NewEncryptedTransport(trans, k.Token, true)
	}
	trans = transport.NewCompressedTransport(trans)
	if err := trans.Start(); err != nil {
		o.ports.release(portIdx)
		return nil, err
	}
	tun := tunnel.NewTCPTunnel(trans, true)
	// See TCPTunnel.SetPortRange's doc comment: every worker on this node
	// shares one real IP and one raw socket's view of all inbound TCP
	// traffic, so without a disjoint range per worker, two keys' stacks
	// could independently pick the same source port at the same time and
	// cross-deliver each other's traffic.
	tun.SetPortRange(portStart, portEnd)
	return &worker{trans: trans, tun: tun, docURL: label, portIdx: portIdx}, nil
}

func (o *Orchestrator) stopWorker(w *worker) {
	w.trans.Stop()
	w.tun.Close()
	o.ports.release(w.portIdx)
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

	disabledNow, err := o.client.ReportUsage(ctx, deltas)
	if err != nil {
		utils.Debugf("[NODEAGENT] report usage failed: %v", err)
		return
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
