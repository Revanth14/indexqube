package orchestrator

import (
	"context"
	"fmt"
	"sort"

	"github.com/Revanth14/indexqube/gateway/internal/agent"
)

type Registry struct {
	backends map[agent.BackendID]agent.Backend
}

var automaticFallbackOrder = []agent.BackendID{agent.BackendCodex, agent.BackendClaude}

func NewRegistry(backends ...agent.Backend) *Registry {
	r := &Registry{backends: make(map[agent.BackendID]agent.Backend)}
	for _, backend := range backends {
		if backend != nil {
			r.backends[backend.ID()] = backend
		}
	}
	return r
}

func (r *Registry) Get(id agent.BackendID) (agent.Backend, error) {
	backend, ok := r.backends[id]
	if !ok {
		return nil, fmt.Errorf("orchestrator: backend %q is not registered", id)
	}
	return backend, nil
}

// NextAutomaticFallback returns the first registered backend in the stable V1
// order that has not already been attempted. Fake and future backends are not
// silently opted into cross-ecosystem fallback.
func (r *Registry) NextAutomaticFallback(current agent.BackendID, attempted map[agent.BackendID]bool) (agent.Backend, bool) {
	if current != agent.BackendCodex && current != agent.BackendClaude {
		return nil, false
	}
	for _, id := range automaticFallbackOrder {
		if attempted[id] {
			continue
		}
		if backend, ok := r.backends[id]; ok {
			return backend, true
		}
	}
	return nil, false
}

func (r *Registry) Health(ctx context.Context) []agent.BackendHealth {
	ids := make([]string, 0, len(r.backends))
	for id := range r.backends {
		ids = append(ids, string(id))
	}
	sort.Strings(ids)
	out := make([]agent.BackendHealth, 0, len(ids))
	for _, id := range ids {
		out = append(out, r.backends[agent.BackendID(id)].Probe(ctx))
	}
	return out
}
