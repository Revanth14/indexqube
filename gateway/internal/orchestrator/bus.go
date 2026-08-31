package orchestrator

import (
	"sync"

	"github.com/Revanth14/indexqube/gateway/internal/agent"
)

type eventBus struct {
	mu          sync.Mutex
	next        uint64
	subscribers map[string]map[uint64]chan agent.Event
}

func newEventBus() *eventBus {
	return &eventBus{subscribers: make(map[string]map[uint64]chan agent.Event)}
}

func (b *eventBus) subscribe(taskID string) (<-chan agent.Event, func()) {
	b.mu.Lock()
	defer b.mu.Unlock()
	b.next++
	id := b.next
	ch := make(chan agent.Event, 128)
	if b.subscribers[taskID] == nil {
		b.subscribers[taskID] = make(map[uint64]chan agent.Event)
	}
	b.subscribers[taskID][id] = ch
	return ch, func() {
		b.mu.Lock()
		defer b.mu.Unlock()
		if subs := b.subscribers[taskID]; subs != nil {
			delete(subs, id)
			if len(subs) == 0 {
				delete(b.subscribers, taskID)
			}
		}
	}
}

func (b *eventBus) publish(event agent.Event) {
	b.mu.Lock()
	defer b.mu.Unlock()
	for _, ch := range b.subscribers[event.TaskID] {
		select {
		case ch <- event:
		default:
			// SQLite is canonical. A slow subscriber can reconnect and replay by
			// sequence rather than blocking an agent process.
		}
	}
}
