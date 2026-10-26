package gateway

import (
	"crypto/sha256"
	"crypto/subtle"
	"errors"
	"mime"
	"net/http"
	"net/url"
	"strconv"
	"strings"
	"time"

	"github.com/rajat-blr/llm-gateway/internal/storage"
)

func (s *Server) serveAdmin(w http.ResponseWriter, r *http.Request) {
	w.Header().Set("Cache-Control", "no-store")
	token, ok := strings.CutPrefix(r.Header.Get("Authorization"), "Bearer ")
	hash := sha256.Sum256([]byte(token))
	if !ok || subtle.ConstantTimeCompare(hash[:], s.adminHash[:]) != 1 {
		s.writeError(w, 401, "invalid_api_key")
		return
	}
	switch {
	case r.URL.Path == "/admin/status" && r.Method == http.MethodGet:
		writeJSON(w, 200, map[string]any{"sqlite_available": s.identities.Ping(r.Context()) == nil, "redis_available": s.limiter.Ping(r.Context()) == nil, "persistence": s.usage.Stats(), "accounting_errors": s.accountingErrors.Load()})
	case r.URL.Path == "/admin/keys" && r.Method == http.MethodPost:
		media, _, err := mime.ParseMediaType(r.Header.Get("Content-Type"))
		if err != nil || media != "application/json" {
			s.writeError(w, 400, "invalid_request")
			return
		}
		b, err := readBounded(r.Body, min(s.cfg.MaxBodyBytes, 64<<10))
		if err != nil {
			s.writeError(w, 400, "invalid_request")
			return
		}
		var spec storage.KeySpec
		if StrictJSON(b, &spec) != nil || spec.Validate() != nil {
			s.writeError(w, 400, "invalid_request")
			return
		}
		for _, alias := range spec.Aliases {
			if _, ok := s.cfg.Routes[alias]; !ok {
				s.writeError(w, 400, "invalid_request")
				return
			}
		}
		key, secret, err := s.identities.CreateKey(r.Context(), spec)
		if err != nil {
			s.writeError(w, 503, "dependency_unavailable")
			return
		}
		writeJSON(w, 201, struct {
			storage.Key
			Secret string `json:"key"`
		}{key, secret})
	case r.URL.Path == "/admin/keys" && r.Method == http.MethodGet:
		if !validQuery(r, "limit", "offset") {
			s.writeError(w, 400, "invalid_request")
			return
		}
		limit, err := intQuery(r, "limit", 100, 1, 100)
		if err != nil {
			s.writeError(w, 400, "invalid_request")
			return
		}
		offset, err := intQuery(r, "offset", 0, 0, 10000)
		if err != nil {
			s.writeError(w, 400, "invalid_request")
			return
		}
		keys, err := s.identities.ListKeys(r.Context(), limit, offset)
		if err != nil {
			s.writeError(w, 503, "dependency_unavailable")
			return
		}
		writeJSON(w, 200, map[string]any{"data": keys, "limit": limit, "offset": offset})
	case strings.HasPrefix(r.URL.Path, "/admin/keys/") && r.Method == http.MethodDelete:
		id := strings.TrimPrefix(r.URL.Path, "/admin/keys/")
		if len(id) > 128 || id == "" || strings.Contains(id, "/") {
			s.writeError(w, 400, "invalid_request")
			return
		}
		err := s.identities.Revoke(r.Context(), id)
		if errors.Is(err, storage.ErrNotFound) {
			s.writeError(w, 404, "invalid_request")
			return
		}
		if err != nil {
			s.writeError(w, 503, "dependency_unavailable")
			return
		}
		w.WriteHeader(204)
	case r.URL.Path == "/admin/usage" && r.Method == http.MethodGet:
		if !validQuery(r, "from", "to", "key_id", "alias", "provider", "limit") {
			s.writeError(w, 400, "invalid_request")
			return
		}
		now := time.Now()
		from, err := timeQuery(r, "from", now.Add(-24*time.Hour))
		if err != nil {
			s.writeError(w, 400, "invalid_request")
			return
		}
		to, err := timeQuery(r, "to", now)
		if err != nil {
			s.writeError(w, 400, "invalid_request")
			return
		}
		limit, err := intQuery(r, "limit", 100, 1, 1000)
		if err != nil {
			s.writeError(w, 400, "invalid_request")
			return
		}
		q := r.URL.Query()
		report, err := s.identities.Usage(r.Context(), storage.UsageQuery{FromMS: from.UnixMilli(), ToMS: to.UnixMilli(), KeyID: q.Get("key_id"), Alias: q.Get("alias"), Provider: q.Get("provider"), Limit: limit})
		if errors.Is(err, storage.ErrInvalidQuery) {
			s.writeError(w, 400, "invalid_request")
			return
		}
		if err != nil {
			s.writeError(w, 503, "dependency_unavailable")
			return
		}
		writeJSON(w, 200, struct {
			storage.UsageReport
			Persistence      storage.Stats `json:"persistence"`
			AccountingErrors uint64        `json:"accounting_errors"`
		}{report, s.usage.Stats(), s.accountingErrors.Load()})
	default:
		s.writeError(w, 404, "invalid_request")
	}
}
func validQuery(r *http.Request, allowed ...string) bool {
	q, err := url.ParseQuery(r.URL.RawQuery)
	if err != nil {
		return false
	}
	for key, values := range q {
		found := false
		for _, name := range allowed {
			if key == name {
				found = true
			}
		}
		if !found || len(values) != 1 || len(values[0]) > 256 {
			return false
		}
	}
	return true
}
func intQuery(r *http.Request, name string, defaultValue, minValue, maxValue int) (int, error) {
	values, ok := r.URL.Query()[name]
	if !ok {
		return defaultValue, nil
	}
	if len(values) != 1 {
		return 0, storage.ErrInvalidQuery
	}
	n, err := strconv.Atoi(values[0])
	if err != nil || n < minValue || n > maxValue {
		return 0, storage.ErrInvalidQuery
	}
	return n, nil
}
func timeQuery(r *http.Request, name string, defaultValue time.Time) (time.Time, error) {
	values, ok := r.URL.Query()[name]
	if !ok {
		return defaultValue, nil
	}
	if len(values) != 1 {
		return time.Time{}, storage.ErrInvalidQuery
	}
	return time.Parse(time.RFC3339Nano, values[0])
}
