package main

import (
	"context"
	"errors"
	"flag"
	"log/slog"
	"net"
	"net/http"
	"os"
	"os/signal"
	"syscall"
	"time"

	"github.com/rajat-blr/llm-gateway/internal/gateway"
)

func main() {
	slog.SetDefault(slog.New(slog.NewJSONHandler(os.Stdout, nil)))
	path := flag.String("config", "configs/demo.json", "routing configuration")
	flag.Parse()
	if err := run(*path); err != nil {
		slog.Error("gateway stopped", "error", err.Error())
		os.Exit(1)
	}
}
func run(path string) error {
	cfg, err := gateway.LoadConfig(path)
	if err != nil {
		return err
	}
	handler, err := gateway.New(cfg)
	if err != nil {
		return err
	}
	defer handler.Close()
	root, cancel := context.WithCancel(context.Background())
	defer cancel()
	server := &http.Server{Addr: cfg.Listen, Handler: handler, ReadHeaderTimeout: 5 * time.Second, ReadTimeout: time.Duration(cfg.RequestTimeout), IdleTimeout: 30 * time.Second, MaxHeaderBytes: 32 << 10, BaseContext: nil}
	// All accepted requests inherit a root canceled when the drain budget expires.
	server.BaseContext = func(_ net.Listener) context.Context { return root }
	signals := make(chan os.Signal, 1)
	signal.Notify(signals, syscall.SIGINT, syscall.SIGTERM)
	defer signal.Stop(signals)
	stopped := make(chan error, 1)
	go func() { stopped <- server.ListenAndServe() }()
	slog.Info("gateway listening", "address", cfg.Listen)
	select {
	case err := <-stopped:
		if errors.Is(err, http.ErrServerClosed) {
			return nil
		}
		return err
	case <-signals:
	}
	handler.StopAdmission()
	ctx, stop := context.WithTimeout(context.Background(), time.Duration(cfg.ShutdownTimeout))
	defer stop()
	err = server.Shutdown(ctx)
	if err != nil {
		cancel()
		_ = server.Close()
	}
	return err
}
