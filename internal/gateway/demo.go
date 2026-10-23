package gateway

import (
	"embed"
	"net/http"
)

//go:embed web/*
var demoFiles embed.FS

// The public landing page is an API demo client. It contains no real keys;
// inference still passes through the normal authenticated /v1 endpoints.
func serveDemo(w http.ResponseWriter, r *http.Request) bool {
	if r.Method != http.MethodGet {
		return false
	}
	var file, contentType string
	switch r.URL.Path {
	case "/":
		file = "index.html"
		contentType = "text/html; charset=utf-8"
	case "/demo.css":
		file = "demo.css"
		contentType = "text/css; charset=utf-8"
	case "/demo.js":
		file = "demo.js"
		contentType = "text/javascript; charset=utf-8"
	default:
		return false
	}
	b, err := demoFiles.ReadFile("web/" + file)
	if err != nil {
		http.Error(w, "Demo unavailable", 500)
		return true
	}
	w.Header().Set("Content-Type", contentType)
	w.Header().Set("Cache-Control", "no-store")
	w.Header().Set("X-Content-Type-Options", "nosniff")
	w.Header().Set("Content-Security-Policy", "default-src 'none'; script-src 'self'; style-src 'self'; connect-src 'self'; base-uri 'none'; form-action 'none'; frame-ancestors 'none'")
	w.Write(b)
	return true
}
