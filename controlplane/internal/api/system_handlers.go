package api

import (
	"net/http"
	"time"

	"openflux-control/internal/sysinfo"
)

// handleSystem reports host metrics of the machine the controlplane runs on
// (CPU, memory, disk, load, uptime), read fresh from /proc on each call via
// the dependency-free sysinfo package. admin-authenticated like every other
// /v1/admin endpoint - system state isn't something to hand to just anyone.
func (a *App) handleSystem(w http.ResponseWriter, r *http.Request) {
	s := sysinfo.Collect(150 * time.Millisecond)
	writeJSON(w, http.StatusOK, s)
}
