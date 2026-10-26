package gateway

import (
	"net/http"
	"net/http/pprof"
	"strconv"
	"time"

	"github.com/prometheus/client_golang/prometheus/promhttp"
)

// ManagementHandler must be served on the separately configured listener.
// It is deliberately absent from the inference router.
func (s *Server) ManagementHandler() http.Handler {
	mux := http.NewServeMux()
	mux.Handle("/metrics", promhttp.HandlerFor(s.metrics.registry, promhttp.HandlerOpts{MaxRequestsInFlight: 2, Timeout: 5 * time.Second}))
	mux.HandleFunc("/healthz", func(w http.ResponseWriter, r *http.Request) { w.WriteHeader(200) })
	if s.cfg.Management.Pprof {
		mux.HandleFunc("/debug/pprof/", pprof.Index)
		mux.HandleFunc("/debug/pprof/cmdline", pprof.Cmdline)
		mux.HandleFunc("/debug/pprof/symbol", pprof.Symbol)
		bounded := func(h http.HandlerFunc, defaultSeconds int) http.HandlerFunc {
			return func(w http.ResponseWriter, r *http.Request) {
				q := r.URL.Query()
				seconds := defaultSeconds
				if raw := q.Get("seconds"); raw != "" {
					n, err := strconv.Atoi(raw)
					if err != nil || n < 1 || n > 30 {
						http.Error(w, "seconds must be between 1 and 30", 400)
						return
					}
					seconds = n
				}
				q.Set("seconds", strconv.Itoa(seconds))
				r.URL.RawQuery = q.Encode()
				h(w, r)
			}
		}
		mux.HandleFunc("/debug/pprof/profile", bounded(pprof.Profile, 10))
		mux.HandleFunc("/debug/pprof/trace", bounded(pprof.Trace, 1))
	}
	slots := make(chan struct{}, 4)
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if raw := r.URL.Query().Get("seconds"); raw != "" {
			n, err := strconv.Atoi(raw)
			if err != nil || n < 1 || n > 30 {
				http.Error(w, "seconds must be between 1 and 30", 400)
				return
			}
		}
		if r.Method != http.MethodGet {
			w.Header().Set("Allow", "GET")
			http.Error(w, "method not allowed", 405)
			return
		}
		select {
		case slots <- struct{}{}:
			defer func() { <-slots }()
			mux.ServeHTTP(w, r)
		default:
			http.Error(w, "management busy", 503)
		}
	})
}
