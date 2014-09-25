package cqrs

import "errors"

// InMemoryEventStreamRepository provides an inmemory event sourcing repository
type InMemoryEventStreamRepository struct {
	store map[string][]VersionedEvent
}

// NewInMemoryEventStreamRepository constructor
func NewInMemoryEventStreamRepository() *InMemoryEventStreamRepository {
	store := make(map[string][]VersionedEvent)
	return &InMemoryEventStreamRepository{store}
}

// Save persists an event sourced object into the repository
func (r *InMemoryEventStreamRepository) Save(id string, newEvents []VersionedEvent) error {
	if events, ok := r.store[id]; ok {
		for _, event := range newEvents {
			events = append(events, event)
		}

		r.store[id] = events
		return nil
	}

	r.store[id] = newEvents
	return nil
}

// Get retrieves events assoicated with an event sourced object by ID
func (r *InMemoryEventStreamRepository) Get(id string, registry TypeRegistry) ([]VersionedEvent, error) {
	if events, ok := r.store[id]; ok {
		return events, nil
	}

	return nil, errors.New("not found")
}
