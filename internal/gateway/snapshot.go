package gateway

import (
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"reflect"
	"time"
)

type routingSnapshot struct {
	config       Config
	generation   uint64
	fingerprints map[string]string
	breakers     map[string]*breaker
}

func (s *Server) buildSnapshot(c Config, generation uint64, old *routingSnapshot) (*routingSnapshot, error) {
	// Round-trip a validated, credential-free config to detach all nested maps
	// and slices from the caller before atomically publishing it.
	b, err := json.Marshal(c)
	if err != nil {
		return nil, err
	}
	var copy Config
	if err = StrictJSON(b, &copy); err != nil {
		return nil, err
	}
	snapshot := &routingSnapshot{config: copy, generation: generation, fingerprints: map[string]string{}, breakers: map[string]*breaker{}}
	for alias, route := range copy.Routes {
		identities := []string{}
		seen := map[string]bool{}
		for _, target := range route.Targets {
			identity := breakerIdentity(copy.Providers[target.Provider], s.secrets[target.Provider], target.Model)
			if seen[identity] {
				return nil, errors.New("route repeats the same upstream credential and model")
			}
			seen[identity] = true
			identities = append(identities, identity)
			var breaker *breaker
			if old != nil {
				breaker = old.breakers[identity]
			}
			if breaker == nil {
				breaker = newBreaker(copy.Breaker, time.Now)
			}
			snapshot.breakers[identity] = breaker
		}
		b, _ := json.Marshal(struct {
			Route      Route
			Identities []string
		}{route, identities})
		hash := sha256.Sum256(b)
		snapshot.fingerprints[alias] = hex.EncodeToString(hash[:])
	}
	return snapshot, nil
}

// Reload accepts route changes only. Process settings and all credential
// definitions remain fixed; rotating credentials requires restarting.
func (s *Server) Reload(path string) error {
	candidate, err := LoadConfig(path)
	if err != nil {
		s.reloadRejected.Add(1)
		return err
	}
	return s.publish(candidate)
}
func (s *Server) publish(candidate Config) error {
	s.reloadMu.Lock()
	defer s.reloadMu.Unlock()
	reject := func(err error) error { s.reloadRejected.Add(1); return err }
	if err := candidate.Validate(); err != nil {
		return reject(err)
	}
	old := s.routing.Load()
	previousStatic, nextStatic := old.config, candidate
	previousStatic.Routes = nil
	nextStatic.Routes = nil
	if !reflect.DeepEqual(previousStatic, nextStatic) {
		return reject(errors.New("non-route configuration changes require restart"))
	}
	for alias, next := range candidate.Routes {
		if previous, ok := old.config.Routes[alias]; ok {
			if next.Revision < previous.Revision {
				return reject(fmt.Errorf("route %q revision cannot decrease", alias))
			}
			left, right := previous, next
			left.Revision = 0
			right.Revision = 0
			if !reflect.DeepEqual(left, right) && next.Revision <= previous.Revision {
				return reject(fmt.Errorf("route %q behavior change requires a higher revision", alias))
			}
		}
	}
	next, err := s.buildSnapshot(candidate, old.generation+1, old)
	if err != nil {
		return reject(err)
	}
	s.routing.Store(next)
	s.reloadAccepted.Add(1)
	return nil
}
