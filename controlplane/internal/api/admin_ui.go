package api

import (
	"embed"
	"net/http"
)

//go:embed web/admin.html
var adminUIFS embed.FS

// The panel page itself carries nothing sensitive - it's a static shell that prompts for the admin bearer token client-side; every actual action still goes through the authenticated JSON endpoints.
func handleAdminUI(w http.ResponseWriter, r *http.Request) {
	data, err := adminUIFS.ReadFile("web/admin.html")
	if err != nil {
		http.Error(w, "admin UI not available", http.StatusInternalServerError)
		return
	}
	w.Header().Set("Content-Type", "text/html; charset=utf-8")
	w.Write(data)
}
