package api

import (
	"net/http"
	"time"

	"openflux-control/internal/sysinfo"
)

func (a *App) handleSystem(w http.ResponseWriter, r *http.Request) {
	s := sysinfo.Collect(150 * time.Millisecond)
	writeJSON(w, http.StatusOK, s)
}
