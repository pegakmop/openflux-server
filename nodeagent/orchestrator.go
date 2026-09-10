// Package nodeagent lets an exit-node process serve many keys at once,
// picking up newly-added or newly-disabled keys from the openflux-control
// service instead of being started with one fixed --url per process.
package nodeagent

import (
	"context"
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
	trans  transport.Transport
	tun    *tunnel.TCPTunnel
	docURL string

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
}

func NewOrchestrator(cfg Config) *Orchestrator {
	return &Orchestrator{
		client:  NewControlClient(cfg.ControlURL, cfg.NodeToken),
		cfg:     cfg,
		workers: make(map[string]*worker),
	}
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
		stopWorker(w)
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
			stopWorker(w)
			delete(o.workers, id)
		}
	}

	for id, k := range active {
		if _, exists := o.workers[id]; exists {
			continue
		}
		if k.Transport != "yandex" {
			utils.Debugf("[NODEAGENT] skipping key %s: managed mode only supports the yandex transport today", id)
			continue
		}

		w, err := startWorker(k)
		if err != nil {
			utils.Debugf("[NODEAGENT] failed to start worker for key %s: %v", id, err)
			continue
		}
		utils.Debugf("[NODEAGENT] started worker for key %s", id)
		o.workers[id] = w
	}
}

func startWorker(k RemoteKey) (*worker, error) {
	trans := transport.NewCompressedTransport(yandex.NewYandexDocsTransport(k.DocURL, transport.DefaultConfig()))
	if err := trans.Start(); err != nil {
		return nil, err
	}
	tun := tunnel.NewTCPTunnel(trans, true)
	return &worker{trans: trans, tun: tun, docURL: k.DocURL}, nil
}

func stopWorker(w *worker) {
	w.trans.Stop()
	w.tun.Close()
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
			stopWorker(w)
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
