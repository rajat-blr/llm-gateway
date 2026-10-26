package storage

import (
	"bytes"
	"context"
	"crypto/sha256"
	"database/sql"
	"os"
	"path/filepath"
	"strings"
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

func TestVersionOneMigrationPreservesKeysAndUsage(t *testing.T) {
	path := filepath.Join(t.TempDir(), "legacy.db")
	db, err := sql.Open("sqlite", path)
	if err != nil {
		t.Fatal(err)
	}
	// This is the released v1 request shape. Other tables are unchanged.
	legacy := `CREATE TABLE requests(id TEXT PRIMARY KEY,key_id TEXT NOT NULL,alias TEXT NOT NULL,provider TEXT NOT NULL,started_ms INTEGER NOT NULL,duration_ms INTEGER NOT NULL,outcome TEXT NOT NULL,client_input INTEGER,client_output INTEGER);`
	if _, err = db.Exec(legacy); err != nil {
		t.Fatal(err)
	}
	if _, err = db.Exec(`INSERT INTO requests VALUES('old','old-key','chat','primary',1000,10,'success',5,3);PRAGMA user_version=1;`); err != nil {
		t.Fatal(err)
	}
	// Supply released key/attempt tables from the stable schema around requests.
	start := strings.Index(schema, "CREATE TABLE IF NOT EXISTS requests")
	end := strings.Index(schema, "CREATE TABLE IF NOT EXISTS attempts")
	if _, err = db.Exec(schema[:start] + schema[end:]); err != nil {
		t.Fatal(err)
	}
	hash := sha256.Sum256([]byte("revoked-legacy-secret"))
	if _, err = db.Exec(`INSERT INTO gateway_keys VALUES('old-key',?,'legacy','fingerprint','["chat"]',100,1,10,0,1000,1001)`, hash[:]); err != nil {
		t.Fatal(err)
	}
	if _, err = db.Exec(`INSERT INTO attempts VALUES('old',1,'primary',1000,10,'success',100,5,3,0)`); err != nil {
		t.Fatal(err)
	}
	db.Close()
	store, err := Open(Options{Path: path, BusyTimeout: time.Millisecond * 50, Timeout: time.Second})
	if err != nil {
		t.Fatal(err)
	}
	defer store.Close()
	var version int
	store.reader.QueryRow("PRAGMA user_version").Scan(&version)
	if version != 2 {
		t.Fatal("migration version not committed")
	}
	if store.EnsureBootstrap(context.Background(), "revoked-legacy-secret", spec()) != nil {
		t.Fatal("bootstrap migration failed")
	}
	if _, err = store.Authenticate(context.Background(), "revoked-legacy-secret"); err != ErrInvalidKey {
		t.Fatal("migration resurrected revoked key")
	}
	report, err := store.Usage(context.Background(), UsageQuery{FromMS: 0, ToMS: 2000, Limit: 10})
	if err != nil || len(report.Requests) != 1 || len(report.Attempts) != 1 || report.Requests[0].ClientOutput != 3 {
		t.Fatalf("migration lost historical usage: %+v: %v", report, err)
	}
	keys, err := store.ListKeys(context.Background(), 10, 0)
	if err != nil || len(keys) != 1 || keys[0].ID != "old-key" || keys[0].RevokedMS == nil {
		t.Fatal("migration changed key identity/metadata")
	}
}
