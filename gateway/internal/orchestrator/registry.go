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
