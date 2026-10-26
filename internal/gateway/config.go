package gateway

import (
	"errors"
	"fmt"
	"net"
	"net/url"
	"os"
	"strings"
	"time"
)

type Duration time.Duration

func (d *Duration) UnmarshalJSON(b []byte) error {
	var s string
	if err := StrictJSON(b, &s); err != nil {
		return err
	}
	v, err := time.ParseDuration(s)
	if err != nil {
		return err
	}
	*d = Duration(v)
	return nil
}

type Config struct {
	Listen          string                    `json:"listen"`
	AdminTokenEnv   string                    `json:"admin_token_env"`
	SQLite          SQLiteConfig              `json:"sqlite"`
	KeyEnv          string                    `json:"key_env"`
	RequestTimeout  Duration                  `json:"request_timeout"`
	IdleTimeout     Duration                  `json:"idle_timeout"`
	WriteTimeout    Duration                  `json:"write_timeout"`
	ShutdownTimeout Duration                  `json:"shutdown_timeout"`
	MaxBodyBytes    int64                     `json:"max_body_bytes"`
	MaxOutputBytes  int64                     `json:"max_output_bytes"`
	MaxEventBytes   int                       `json:"max_event_bytes"`
	MaxConcurrent   int                       `json:"max_concurrent"`
	MaxStreams      int                       `json:"max_streams"`
	Redis           RedisConfig               `json:"redis"`
	Limit           LimitConfig               `json:"token_limit"`
	Breaker         BreakerConfig             `json:"breaker"`
	BackoffBase     Duration                  `json:"backoff_base"`
	BackoffMax      Duration                  `json:"backoff_max"`
	Providers       map[string]ProviderConfig `json:"providers"`
	Routes          map[string]Route          `json:"routes"`
}
type SQLiteConfig struct {
	Path        string   `json:"path"`
	Timeout     Duration `json:"timeout"`
	BusyTimeout Duration `json:"busy_timeout"`
	UsageQueue  int      `json:"usage_queue"`
}
type ProviderConfig struct {
	Kind    string `json:"kind"`
	BaseURL string `json:"base_url"`
	KeyEnv  string `json:"key_env"`
}
type RedisConfig struct {
	Address        string   `json:"address"`
	PasswordEnv    string   `json:"password_env,omitempty"`
	DB             int      `json:"db"`
	Prefix         string   `json:"prefix"`
	Timeout        Duration `json:"timeout"`
	ReservationTTL Duration `json:"reservation_ttl"`
}
type LimitConfig struct {
	Capacity        int64 `json:"capacity"`
	RefillPerSecond int64 `json:"refill_per_second"`
}
type Target struct {
	Provider       string   `json:"provider"`
	Model          string   `json:"model"`
	AttemptTimeout Duration `json:"attempt_timeout"`
}
type Route struct {
	Targets         []Target `json:"targets"`
	MaxOutputTokens int      `json:"max_output_tokens"`
}

func LoadConfig(path string) (Config, error) {
	var c Config
	b, err := os.ReadFile(path)
	if err != nil {
		return c, err
	}
	if len(b) > 1<<20 {
		return c, errors.New("configuration too large")
	}
	if err = StrictJSON(b, &c); err != nil {
		return c, err
	}
	return c, c.Validate()
}
func (c Config) Validate() error {
	if c.AdminTokenEnv == "" || c.AdminTokenEnv == c.KeyEnv || c.SQLite.Path == "" || c.SQLite.BusyTimeout < Duration(time.Millisecond) || c.SQLite.Timeout < c.SQLite.BusyTimeout || c.SQLite.Timeout > Duration(time.Second) || c.SQLite.UsageQueue < 1 || c.SQLite.UsageQueue > 10000 {
		return errors.New("invalid SQLite/admin configuration")
	}
	if c.Listen == "" || c.KeyEnv == "" || len(c.Routes) == 0 || len(c.Providers) == 0 {
		return errors.New("listen, key_env, routes and providers required")
	}
	if c.RequestTimeout <= 0 || c.RequestTimeout > Duration(10*time.Minute) || c.IdleTimeout <= 0 || c.IdleTimeout > c.RequestTimeout || c.WriteTimeout <= 0 || c.WriteTimeout > c.RequestTimeout || c.ShutdownTimeout <= 0 || c.ShutdownTimeout > Duration(time.Minute) {
		return errors.New("invalid timeout bounds")
	}
	if c.MaxBodyBytes < 1 || c.MaxBodyBytes > 4<<20 || c.MaxOutputBytes < 1 || c.MaxOutputBytes > 64<<20 || c.MaxEventBytes < 128 || c.MaxEventBytes > 1<<20 || c.MaxConcurrent < 1 || c.MaxConcurrent > 4096 || c.MaxStreams < 1 || c.MaxStreams > c.MaxConcurrent {
		return errors.New("invalid resource limits")
	}
	for name, p := range c.Providers {
		u, err := url.Parse(p.BaseURL)
		if err != nil || u.Host == "" || (u.Scheme != "http" && u.Scheme != "https") || u.User != nil || u.RawQuery != "" || u.Fragment != "" || p.KeyEnv == "" || (p.Kind != "openai" && p.Kind != "anthropic") {
			return fmt.Errorf("invalid provider %q", name)
		}
	}
	if _, _, err := net.SplitHostPort(c.Redis.Address); err != nil {
		return errors.New("Redis address must be host:port")
	}
	if c.Redis.Address == "" || c.Redis.Prefix == "" || strings.ContainsAny(c.Redis.Prefix, "{}") || c.Redis.DB < 0 || c.Redis.DB > 15 || c.Redis.Timeout <= 0 || c.Redis.Timeout > Duration(time.Second) || c.Redis.ReservationTTL < c.RequestTimeout+c.ShutdownTimeout+2*c.Redis.Timeout || c.Redis.ReservationTTL > Duration(24*time.Hour) {
		return errors.New("invalid Redis configuration")
	}
	if c.Limit.Capacity < 1 || c.Limit.Capacity > 1_000_000_000 || c.Limit.RefillPerSecond < 1 || c.Limit.RefillPerSecond > 1_000_000 {
		return errors.New("invalid token limit")
	}
	if c.Breaker.Failures < 1 || c.Breaker.Failures > 1000 || c.Breaker.Window <= 0 || c.Breaker.Window > Duration(time.Hour) || c.Breaker.OpenFor <= 0 || c.Breaker.OpenFor > Duration(time.Hour) || c.Breaker.RecoverySuccesses < 1 || c.Breaker.RecoverySuccesses > 100 {
		return errors.New("invalid breaker policy")
	}
	if c.BackoffBase <= 0 || c.BackoffMax < c.BackoffBase || c.BackoffMax > c.RequestTimeout {
		return errors.New("invalid retry backoff")
	}
	for name, r := range c.Routes {
		if name == "" || len(name) > 128 || r.MaxOutputTokens < 1 || r.MaxOutputTokens > 32768 || len(r.Targets) < 1 || len(r.Targets) > 2 {
			return fmt.Errorf("invalid route %q", name)
		}
		seen := map[string]bool{}
		for _, target := range r.Targets {
			p, ok := c.Providers[target.Provider]
			identity := p.Kind + "\x00" + p.BaseURL + "\x00" + p.KeyEnv + "\x00" + target.Model
			if !ok || target.Model == "" || target.AttemptTimeout <= 0 || target.AttemptTimeout > c.RequestTimeout || seen[identity] {
				return fmt.Errorf("invalid or duplicate target in route %q", name)
			}
			seen[identity] = true
		}
	}
	return nil
}
