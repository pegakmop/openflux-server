package api

import (
	"context"
	"net"
	"net/http"
	"sync"
	"time"

	"openflux-control/internal/auth"
	"openflux-control/internal/model"
	"openflux-control/internal/store"
)

type ctxKey int

const (
	ctxKeyNode ctxKey = iota
	ctxKeyIngestToken
)

func nodeFromContext(ctx context.Context) model.Node {
	n, _ := ctx.Value(ctxKeyNode).(model.Node)
	return n
}

func ingestTokenFromContext(ctx context.Context) model.IngestToken {
	t, _ := ctx.Value(ctxKeyIngestToken).(model.IngestToken)
	return t
}

// withAdmin requires the bootstrap admin bearer token. There is exactly one
// admin token (from config), so this is a constant-time string compare
// rather than a DB lookup.
func (a *App) withAdmin(next http.HandlerFunc) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		token, ok := auth.ExtractBearer(r)
		if !ok || !auth.ConstantTimeEqual(token, a.Config.AdminToken) {
			writeError(w, http.StatusUnauthorized, "invalid admin token")
			return
		}
		next(w, r)
	}
}

func (a *App) withIngestToken(next http.HandlerFunc) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		token, ok := auth.ExtractBearer(r)
		if !ok {
			writeError(w, http.StatusUnauthorized, "missing bearer token")
			return
		}

		it, err := a.Store.GetIngestTokenByHash(r.Context(), a.Hasher.Hash(token))
		if err == store.ErrNotFound {
			writeError(w, http.StatusUnauthorized, "invalid ingest token")
			return
		}
		if err != nil {
			writeInternalError(w, r, "lookup failed", err)
			return
		}

		ctx := context.WithValue(r.Context(), ctxKeyIngestToken, it)
		next(w, r.WithContext(ctx))
	}
}

func (a *App) withNodeToken(next http.HandlerFunc) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		token, ok := auth.ExtractBearer(r)
		if !ok {
			writeError(w, http.StatusUnauthorized, "missing bearer token")
			return
		}

		n, err := a.Store.GetNodeBySecretHash(r.Context(), a.Hasher.Hash(token))
		if err == store.ErrNotFound {
			writeError(w, http.StatusUnauthorized, "invalid node token")
			return
		}
		if err != nil {
			writeInternalError(w, r, "lookup failed", err)
			return
		}

		// "me" lets a node address itself without needing to know its own
		// generated ID up front; an explicit ID is still checked to guard
		// against a node token being used against another node's path.
		if pathID := r.PathValue("id"); pathID != "" && pathID != "me" && pathID != n.ID {
			writeError(w, http.StatusForbidden, "node token does not match node id")
			return
		}

		ctx := context.WithValue(r.Context(), ctxKeyNode, n)
		next(w, r.WithContext(ctx))
	}
}

func (a *App) withRateLimit(next http.HandlerFunc) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		ip := clientIP(r)
		if !a.limiter.allow(ip) {
			writeError(w, http.StatusTooManyRequests, "rate limit exceeded")
			return
		}
		next(w, r)
	}
}

func clientIP(r *http.Request) string {
	host, _, err := net.SplitHostPort(r.RemoteAddr)
	if err != nil {
		return r.RemoteAddr
	}
	return host
}

// ipRateLimiter is a minimal per-IP token bucket. It is intentionally
// dependency-free and holds state only in memory: at controlplane's expected
// scale (control traffic only, not tunneled data) an unbounded map of
// recently-seen IPs is acceptable, but it does mean limits reset on restart
// and are not shared across replicas - fine for blunting casual token
// brute-forcing on /v1/resolve, not a substitute for a real WAF.
type ipRateLimiter struct {
	mu      sync.Mutex
	buckets map[string]*bucket
	rps     float64
	burst   int
}

type bucket struct {
	tokens   float64
	lastSeen time.Time
}

func newIPRateLimiter(rps float64, burst int) *ipRateLimiter {
	if rps <= 0 {
		rps = 1
	}
	if burst <= 0 {
		burst = 1
	}
	return &ipRateLimiter{
		buckets: make(map[string]*bucket),
		rps:     rps,
		burst:   burst,
	}
}

func (l *ipRateLimiter) allow(ip string) bool {
	l.mu.Lock()
	defer l.mu.Unlock()

	now := time.Now()
	b, ok := l.buckets[ip]
	if !ok {
		b = &bucket{tokens: float64(l.burst), lastSeen: now}
		l.buckets[ip] = b
	}

	elapsed := now.Sub(b.lastSeen).Seconds()
	b.tokens = min(float64(l.burst), b.tokens+elapsed*l.rps)
	b.lastSeen = now

	if b.tokens < 1 {
		return false
	}
	b.tokens--
	return true
}
