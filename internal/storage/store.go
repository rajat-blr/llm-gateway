// Package storage owns SQLite keys and best-effort usage records. It never
// stores plaintext credentials, prompts, completions, or upstream error bodies.
package storage

import (
	"context"
	"crypto/rand"
	"crypto/sha256"
	"database/sql"
	_ "embed"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"net/url"
	"os"
	"path/filepath"
	"time"

	_ "modernc.org/sqlite"
)

//go:embed schema.sql
var schema string
var ErrUnavailable = errors.New("SQLite unavailable")
var ErrInvalidKey = errors.New("invalid gateway key")
var ErrNotFound = errors.New("key not found")

type Options struct {
	Path                 string
	BusyTimeout, Timeout time.Duration
}
type Store struct {
	writer, reader *sql.DB
	timeout        time.Duration
}
type KeySpec struct {
	Name            string   `json:"name"`
	Aliases         []string `json:"aliases"`
	Capacity        int64    `json:"capacity"`
	RefillPerSecond int64    `json:"refill_per_second"`
	MaxOutputTokens int      `json:"max_output_tokens"`
	AllowReuse      bool     `json:"allow_reuse"`
}
type Key struct {
	ID string `json:"id"`
	KeySpec
	Prefix    string `json:"prefix"`
	CreatedMS int64  `json:"created_ms"`
	RevokedMS *int64 `json:"revoked_ms"`
}

func (k KeySpec) Validate() error {
	if len(k.Name) < 1 || len(k.Name) > 128 || len(k.Aliases) < 1 || len(k.Aliases) > 128 || k.Capacity < 1 || k.Capacity > 1_000_000_000 || k.RefillPerSecond < 1 || k.RefillPerSecond > 1_000_000 || k.MaxOutputTokens < 1 || k.MaxOutputTokens > 32768 {
		return errors.New("invalid key policy")
	}
	seen := map[string]bool{}
	for _, alias := range k.Aliases {
		if len(alias) == 0 || len(alias) > 128 || seen[alias] {
			return errors.New("invalid aliases")
		}
		seen[alias] = true
	}
	return nil
}
func Open(o Options) (*Store, error) {
	if o.Path == "" || o.BusyTimeout <= 0 || o.Timeout < o.BusyTimeout {
		return nil, errors.New("invalid SQLite options")
	}
	path, err := filepath.Abs(o.Path)
	if err != nil {
		return nil, ErrUnavailable
	}
	if err = os.MkdirAll(filepath.Dir(path), 0700); err != nil {
		return nil, ErrUnavailable
	}
	file, err := os.OpenFile(path, os.O_CREATE|os.O_RDWR, 0600)
	if err != nil {
		return nil, ErrUnavailable
	}
	file.Close()
	u := url.URL{Scheme: "file", Path: path}
	q := url.Values{}
	q.Set("mode", "rw")
	q.Add("_pragma", fmt.Sprintf("busy_timeout(%d)", o.BusyTimeout.Milliseconds()))
	q.Add("_pragma", "foreign_keys(1)")
	q.Add("_pragma", "synchronous(FULL)")
	u.RawQuery = q.Encode()
	writer, err := sql.Open("sqlite", u.String())
	if err != nil {
		return nil, ErrUnavailable
	}
	writer.SetMaxOpenConns(1)
	writer.SetMaxIdleConns(1)
	s := &Store{writer: writer, timeout: o.Timeout}
	ok := false
	defer func() {
		if !ok {
			writer.Close()
			if s.reader != nil {
				s.reader.Close()
			}
		}
	}()
	ctx, cancel := context.WithTimeout(context.Background(), o.Timeout)
	defer cancel()
	var version int
	if err = writer.QueryRowContext(ctx, "PRAGMA user_version").Scan(&version); err != nil || version > 1 {
		return nil, ErrUnavailable
	}
	var mode string
	if err = writer.QueryRowContext(ctx, "PRAGMA journal_mode=WAL").Scan(&mode); err != nil || mode != "wal" {
		return nil, ErrUnavailable
	}
	tx, err := writer.BeginTx(ctx, nil)
	if err != nil {
		return nil, ErrUnavailable
	}
	defer tx.Rollback()
	if _, err = tx.ExecContext(ctx, schema); err != nil {
		return nil, ErrUnavailable
	}
	if _, err = tx.ExecContext(ctx, "PRAGMA user_version=1"); err != nil {
		return nil, ErrUnavailable
	}
	if err = tx.Commit(); err != nil {
		return nil, ErrUnavailable
	}
	s.reader, err = sql.Open("sqlite", u.String())
	if err != nil {
		return nil, ErrUnavailable
	}
	s.reader.SetMaxOpenConns(4)
	s.reader.SetMaxIdleConns(4)
	if s.Ping(ctx) != nil {
		return nil, ErrUnavailable
	}
	ok = true
	return s, nil
}
func (s *Store) Ping(ctx context.Context) error {
	ctx, cancel := context.WithTimeout(ctx, s.timeout)
	defer cancel()
	var n int
	if s.reader.QueryRowContext(ctx, "SELECT count(*) FROM gateway_keys WHERE 0").Scan(&n) != nil {
		return ErrUnavailable
	}
	return nil
}
func (s *Store) Close() error {
	a := s.reader.Close()
	b := s.writer.Close()
	if a != nil {
		return a
	}
	return b
}
func randomID(prefix string, n int) string {
	b := make([]byte, n)
	if _, err := rand.Read(b); err != nil {
		panic("crypto/rand unavailable")
	}
	return prefix + hex.EncodeToString(b)
}

const keyColumns = "id,name,prefix,aliases,capacity,refill,max_output_tokens,allow_reuse,created_ms,revoked_ms"

type scanner interface{ Scan(...any) error }

func scanKey(row scanner) (Key, error) {
	var k Key
	var aliases string
	err := row.Scan(&k.ID, &k.Name, &k.Prefix, &aliases, &k.Capacity, &k.RefillPerSecond, &k.MaxOutputTokens, &k.AllowReuse, &k.CreatedMS, &k.RevokedMS)
	if errors.Is(err, sql.ErrNoRows) {
		return k, ErrInvalidKey
	}
	if err != nil {
		return k, ErrUnavailable
	}
	if json.Unmarshal([]byte(aliases), &k.Aliases) != nil || k.KeySpec.Validate() != nil {
		return k, ErrUnavailable
	}
	return k, nil
}
func (s *Store) Authenticate(ctx context.Context, secret string) (Key, error) {
	ctx, cancel := context.WithTimeout(ctx, s.timeout)
	defer cancel()
	hash := sha256.Sum256([]byte(secret))
	return scanKey(s.reader.QueryRowContext(ctx, "SELECT "+keyColumns+" FROM gateway_keys WHERE secret_hash=? AND revoked_ms IS NULL", hash[:]))
}
func (s *Store) insert(ctx context.Context, secret string, spec KeySpec) (Key, error) {
	k := Key{ID: randomID("key_", 16), KeySpec: spec, CreatedMS: time.Now().UnixMilli()}
	hash := sha256.Sum256([]byte(secret))
	k.Prefix = "sha256:" + hex.EncodeToString(hash[:4])
	aliases, _ := json.Marshal(spec.Aliases)
	_, err := s.writer.ExecContext(ctx, `INSERT INTO gateway_keys(id,secret_hash,name,prefix,aliases,capacity,refill,max_output_tokens,allow_reuse,created_ms) VALUES(?,?,?,?,?,?,?,?,?,?)`, k.ID, hash[:], k.Name, k.Prefix, string(aliases), k.Capacity, k.RefillPerSecond, k.MaxOutputTokens, k.AllowReuse, k.CreatedMS)
	if err != nil {
		return Key{}, ErrUnavailable
	}
	return k, nil
}
func (s *Store) CreateKey(ctx context.Context, spec KeySpec) (Key, string, error) {
	if err := spec.Validate(); err != nil {
		return Key{}, "", err
	}
	ctx, cancel := context.WithTimeout(ctx, s.timeout)
	defer cancel()
	secret := randomID("gw_", 32)
	k, err := s.insert(ctx, secret, spec)
	if err != nil {
		return Key{}, "", err
	}
	return k, secret, nil
}

// EnsureBootstrap only imports a missing secret. A revoked import is never
// reactivated, and restart never overwrites its durable policy.
func (s *Store) EnsureBootstrap(ctx context.Context, secret string, spec KeySpec) error {
	if secret == "" {
		return nil
	}
	if spec.Validate() != nil {
		return errors.New("invalid bootstrap policy")
	}
	ctx, cancel := context.WithTimeout(ctx, s.timeout)
	defer cancel()
	hash := sha256.Sum256([]byte(secret))
	var id string
	err := s.reader.QueryRowContext(ctx, "SELECT id FROM gateway_keys WHERE secret_hash=?", hash[:]).Scan(&id)
	if err == nil {
		return nil
	}
	if !errors.Is(err, sql.ErrNoRows) {
		return ErrUnavailable
	}
	_, err = s.insert(ctx, secret, spec)
	if err != nil { // Another process can import the same bootstrap concurrently.
		if s.reader.QueryRowContext(ctx, "SELECT id FROM gateway_keys WHERE secret_hash=?", hash[:]).Scan(&id) == nil {
			return nil
		}
	}
	return err
}
func (s *Store) ListKeys(ctx context.Context, limit, offset int) ([]Key, error) {
	if limit < 1 || limit > 100 || offset < 0 || offset > 10000 {
		return nil, errors.New("invalid pagination")
	}
	ctx, cancel := context.WithTimeout(ctx, s.timeout)
	defer cancel()
	rows, err := s.reader.QueryContext(ctx, "SELECT "+keyColumns+" FROM gateway_keys ORDER BY created_ms,id LIMIT ? OFFSET ?", limit, offset)
	if err != nil {
		return nil, ErrUnavailable
	}
	defer rows.Close()
	keys := []Key{}
	for rows.Next() {
		k, err := scanKey(rows)
		if err != nil {
			return nil, err
		}
		keys = append(keys, k)
	}
	if rows.Err() != nil {
		return nil, ErrUnavailable
	}
	return keys, nil
}
func (s *Store) Revoke(ctx context.Context, id string) error {
	ctx, cancel := context.WithTimeout(ctx, s.timeout)
	defer cancel()
	result, err := s.writer.ExecContext(ctx, "UPDATE gateway_keys SET revoked_ms=COALESCE(revoked_ms,?) WHERE id=?", time.Now().UnixMilli(), id)
	if err != nil {
		return ErrUnavailable
	}
	n, err := result.RowsAffected()
	if err != nil {
		return ErrUnavailable
	}
	if n == 0 {
		return ErrNotFound
	}
	return nil
}
