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
	apiListener, err := net.Listen("tcp", cfg.Listen)
	if err != nil {
		return err
	}
	defer apiListener.Close()
	var management *http.Server
	var managementListener net.Listener
	if cfg.Management.Listen != "" {
		managementListener, err = net.Listen("tcp", cfg.Management.Listen)
		if err != nil {
			return err
		}
		defer managementListener.Close()
		management = &http.Server{Handler: handler.ManagementHandler(), ReadHeaderTimeout: 5 * time.Second, ReadTimeout: 5 * time.Second, WriteTimeout: 35 * time.Second, IdleTimeout: 30 * time.Second, MaxHeaderBytes: 32 << 10, BaseContext: func(net.Listener) context.Context { return root }}
	}
	signals := make(chan os.Signal, 8)
	signal.Notify(signals, syscall.SIGINT, syscall.SIGTERM, syscall.SIGHUP)
	defer signal.Stop(signals)
	stopped := make(chan error, 2)
	go func() { stopped <- server.Serve(apiListener) }()
	if management != nil {
		go func() { stopped <- management.Serve(managementListener) }()
		slog.Info("management listening", "address", cfg.Management.Listen, "pprof", cfg.Management.Pprof)
	}
	defer server.Close()
	if management != nil {
		defer management.Close()
	}
	slog.Info("gateway listening", "address", cfg.Listen)
waiting:
	for {
		select {
		case err := <-stopped:
			if errors.Is(err, http.ErrServerClosed) {
				return nil
			}
			return err
		case signal := <-signals:
			if signal == syscall.SIGHUP {
				if err := handler.Reload(path); err != nil {
					slog.Error("routing reload rejected", "error", err.Error())
				} else {
					slog.Info("routing reload accepted")
				}
				continue
			}
			break waiting
		}
	}

	handler.StopAdmission()
	ctx, stop := context.WithTimeout(context.Background(), time.Duration(cfg.ShutdownTimeout))
	defer stop()
	drains := make(chan error, 2)
	go func() { drains <- server.Shutdown(ctx) }()
	count := 1
	if management != nil {
		count++
		go func() { drains <- management.Shutdown(ctx) }()
	}
	for range count {
		if drainErr := <-drains; drainErr != nil {
			err = errors.Join(err, drainErr)
		}
	}
	if err != nil {
		cancel()
		_ = server.Close()
		if management != nil {
			_ = management.Close()
		}
	}
	usageErr := handler.Shutdown(ctx)
	if err == nil {
		err = usageErr
	}
	return err
}
