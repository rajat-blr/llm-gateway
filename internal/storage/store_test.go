package storage

import (
	"bytes"
	"context"
	"database/sql"
	"os"
	"path/filepath"
	"sync"
	"testing"
	"time"
)

func openTest(t *testing.T) (*Store, Options) {
	t.Helper()
	o := Options{Path: filepath.Join(t.TempDir(), "gateway.db"), BusyTimeout: 30 * time.Millisecond, Timeout: 300 * time.Millisecond}
	s, err := Open(o)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { s.Close() })
	return s, o
}
func spec() KeySpec {
	return KeySpec{Name: "test", Aliases: []string{"chat"}, Capacity: 1000, RefillPerSecond: 10, MaxOutputTokens: 100}
}
func TestHashedKeysRevocationAndRestart(t *testing.T) {
	s, o := openTest(t)
	ctx := context.Background()
	key, secret, err := s.CreateKey(ctx, spec())
	if err != nil {
		t.Fatal(err)
	}
	found, err := s.Authenticate(ctx, secret)
	if err != nil || found.ID != key.ID {
		t.Fatal("created key cannot authenticate")
	}
	keys, err := s.ListKeys(ctx, 100, 0)
	if err != nil || len(keys) != 1 {
		t.Fatal(err)
	}
	for _, suffix := range []string{"", "-wal", "-shm"} {
		b, err := os.ReadFile(o.Path + suffix)
		if err != nil && !os.IsNotExist(err) {
			t.Fatal(err)
		}
		if bytes.Contains(b, []byte(secret)) {
			t.Fatal("plaintext credential stored")
		}
	}
	second, err := Open(o)
	if err != nil {
		t.Fatal(err)
	}
	defer second.Close()
	if err = s.Revoke(ctx, key.ID); err != nil {
		t.Fatal(err)
	}
	if _, err = second.Authenticate(ctx, secret); err != ErrInvalidKey {
		t.Fatalf("revocation not visible across connections: %v", err)
	}
	if s.Revoke(ctx, key.ID) != nil {
		t.Fatal("repeat revocation was not idempotent")
	}
	s.Close()
	reopened, err := Open(o)
	if err != nil {
		t.Fatal(err)
	}
	defer reopened.Close()
	if reopened.EnsureBootstrap(ctx, secret, spec()) != nil {
		t.Fatal("bootstrap lookup failed")
	}
	if _, err = reopened.Authenticate(ctx, secret); err != ErrInvalidKey {
		t.Fatal("restart reactivated revoked key")
	}
}
func TestBootstrapIsStableAndDoesNotOverwritePolicy(t *testing.T) {
	s, _ := openTest(t)
	ctx := context.Background()
	if s.EnsureBootstrap(ctx, "initial-secret", spec()) != nil {
		t.Fatal("import failed")
	}
	a, err := s.Authenticate(ctx, "initial-secret")
	if err != nil {
		t.Fatal(err)
	}
	changed := spec()
	changed.Capacity = 99999
	if s.EnsureBootstrap(ctx, "initial-secret", changed) != nil {
		t.Fatal("repeat import failed")
	}
	b, _ := s.Authenticate(ctx, "initial-secret")
	if a.ID != b.ID || b.Capacity != 1000 {
		t.Fatal("bootstrap identity/policy changed on restart")
	}
}
func TestWALReadsAndBoundedWriteContention(t *testing.T) {
	s, o := openTest(t)
	ctx := context.Background()
	_, secret, err := s.CreateKey(ctx, spec())
	if err != nil {
		t.Fatal(err)
	}
	other, err := sql.Open("sqlite", o.Path)
	if err != nil {
		t.Fatal(err)
	}
	defer other.Close()
	other.SetMaxOpenConns(1)
	tx, err := other.BeginTx(ctx, nil)
	if err != nil {
		t.Fatal(err)
	}
	defer tx.Rollback()
	if _, err = tx.Exec("UPDATE gateway_keys SET name=name"); err != nil {
		t.Fatal(err)
	}
	var wg sync.WaitGroup
	failures := make(chan error, 20)
	for i := 0; i < 20; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			_, err := s.Authenticate(ctx, secret)
			if err != nil {
				failures <- err
			}
		}()
	}
	wg.Wait()
	close(failures)
	for err := range failures {
		t.Fatalf("WAL read blocked by writer: %v", err)
	}
	started := time.Now()
	if _, _, err = s.CreateKey(ctx, spec()); err != ErrUnavailable {
		t.Fatalf("write contention did not fail visibly: %v", err)
	}
	if time.Since(started) > 500*time.Millisecond {
		t.Fatal("busy timeout was unbounded")
	}
	tx.Rollback()
	if _, _, err = s.CreateKey(ctx, spec()); err != nil {
		t.Fatal("writer did not recover")
	}
}
func TestUsageSeparatesAttemptsAndClientResults(t *testing.T) {
	s, _ := openTest(t)
	ctx := context.Background()
	in, out := 5, 3
	now := time.Now().UnixMilli()
	r := Record{ID: "request", KeyID: "key", Alias: "chat", Provider: "backup", Outcome: "success", StartedMS: now, DurationMS: 10, ClientInput: &in, ClientOutput: &out, Attempts: []Attempt{{Number: 1, Provider: "primary", StartedMS: now, Outcome: "upstream_failure", ReservedTokens: 100}, {Number: 2, Provider: "backup", StartedMS: now, Outcome: "success", ReservedTokens: 100, InputTokens: &in, OutputTokens: &out}}}
	if err := s.WriteUsage(ctx, r); err != nil {
		t.Fatal(err)
	}
	report, err := s.Usage(ctx, UsageQuery{FromMS: now - 1, ToMS: now + 1, Limit: 100})
	if err != nil {
		t.Fatal(err)
	}
	if len(report.Requests) != 1 || report.Requests[0].Requests != 1 || report.Requests[0].ClientInput != 5 || len(report.Attempts) != 2 {
		t.Fatalf("wrong aggregation: %+v", report)
	}
	var known, unknown, input int64
	for _, a := range report.Attempts {
		known += a.KnownUsage
		unknown += a.UnknownUsage
		input += a.Input
	}
	if known != 1 || unknown != 1 || input != 5 {
		t.Fatal("unknown attempts counted as zero usage or client usage double counted")
	}
	filtered, err := s.Usage(ctx, UsageQuery{FromMS: now - 1, ToMS: now + 1, Provider: "primary", Limit: 100})
	if err != nil || len(filtered.Attempts) != 1 || len(filtered.Requests) != 0 {
		t.Fatal("provider filter mixed client and attempt providers")
	}
	invalid := r
	invalid.ID = "rollback"
	invalid.Attempts[0].Number = 3
	if s.WriteUsage(ctx, invalid) != ErrUnavailable {
		t.Fatal("invalid attempt accepted")
	}
	var count int
	if s.reader.QueryRow("SELECT count(*) FROM requests WHERE id='rollback'").Scan(&count) != nil || count != 0 {
		t.Fatal("partial usage record committed")
	}
	limited, err := s.Usage(ctx, UsageQuery{FromMS: now - 1, ToMS: now + 1, Limit: 1})
	if err != nil || !limited.Truncated {
		t.Fatal("aggregation silently truncated")
	}
}
func TestClosedStoreIsUnavailableNotInvalidKey(t *testing.T) {
	s, _ := openTest(t)
	s.Close()
	if _, err := s.Authenticate(context.Background(), "any"); err != ErrUnavailable {
		t.Fatalf("closed DB: %v", err)
	}
	if s.Ping(context.Background()) != ErrUnavailable {
		t.Fatal("closed DB was ready")
	}
}
