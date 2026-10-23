package gateway

import (
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
)

func TestPublicDemoKeepsAPIAuthenticated(t *testing.T) {
	upstream := httptest.NewServer(http.NotFoundHandler())
	defer upstream.Close()
	gateway := testGateway(t, testConfig(t, upstream.URL, "openai"))
	for path, contentType := range map[string]string{"/": "text/html", "/demo.js": "text/javascript", "/demo.css": "text/css"} {
		response, err := http.Get(gateway.URL + path)
		if err != nil {
			t.Fatal(err)
		}
		b := body(t, response)
		if response.StatusCode != 200 || !strings.HasPrefix(response.Header.Get("Content-Type"), contentType) || b == "" {
			t.Fatalf("%s: %d", path, response.StatusCode)
		}
		if response.Header.Get("Content-Security-Policy") == "" || response.Header.Get("Cache-Control") != "no-store" {
			t.Fatal("missing browser security headers")
		}
		if strings.Contains(b, "provider-test-secret") || strings.Contains(b, "gateway-test-secret") {
			t.Fatal("server credential leaked in demo")
		}
	}
	for _, path := range []string{"/v1/models", "/v1/chat/completions"} {
		response, err := http.Get(gateway.URL + path)
		if err != nil {
			t.Fatal(err)
		}
		response.Body.Close()
		if response.StatusCode != 401 {
			t.Fatalf("API authentication bypass: %s: %d", path, response.StatusCode)
		}
	}
}
