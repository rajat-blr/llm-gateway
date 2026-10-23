package gateway

import (
	"errors"
	"fmt"
	"net/url"
	"os"
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
	Providers       map[string]ProviderConfig `json:"providers"`
	Routes          map[string]Route          `json:"routes"`
}
type ProviderConfig struct {
	Kind    string `json:"kind"`
	BaseURL string `json:"base_url"`
	KeyEnv  string `json:"key_env"`
}
type Route struct {
	Provider        string `json:"provider"`
	Model           string `json:"model"`
	MaxOutputTokens int    `json:"max_output_tokens"`
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
	for name, r := range c.Routes {
		if _, ok := c.Providers[r.Provider]; !ok || name == "" || r.Model == "" || r.MaxOutputTokens < 1 || r.MaxOutputTokens > 32768 {
			return fmt.Errorf("invalid route %q", name)
		}
	}
	return nil
}
