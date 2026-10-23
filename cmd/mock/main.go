package main

import (
	"flag"
	"log/slog"
	"net/http"
	"os"
	"time"

	mockprovider "github.com/rajat-blr/llm-gateway/internal/mock"
)

func main() {
	addr := flag.String("listen", ":9090", "listen address")
	first := flag.Duration("first-delay", 20*time.Millisecond, "first-event delay")
	interval := flag.Duration("chunk-delay", 30*time.Millisecond, "delay between chunks")
	chunks := flag.Int("chunks", 3, "text chunks per response")
	payload := flag.String("payload", "hello ", "text in each chunk")
	failures := flag.Uint64("fail-every", 0, "return 503 every Nth request; 0 disables")
	drop := flag.Int("drop-after", 0, "drop stream after N text chunks; 0 disables")
	flag.Parse()
	handler := mockprovider.New(mockprovider.Options{FirstDelay: *first, ChunkDelay: *interval, Chunks: *chunks, Payload: *payload, FailEvery: *failures, DropAfter: *drop})
	server := &http.Server{Addr: *addr, Handler: handler, ReadHeaderTimeout: 5 * time.Second, ReadTimeout: 10 * time.Second, IdleTimeout: 30 * time.Second, MaxHeaderBytes: 32 << 10}
	slog.Info("mock listening", "address", *addr)
	if err := server.ListenAndServe(); err != nil {
		slog.Error("mock stopped", "error", err)
		os.Exit(1)
	}
}
