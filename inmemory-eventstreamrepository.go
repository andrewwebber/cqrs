package cqrs

import "errors"

type InMemoryEventStreamRepository struct {
	store map[string][]VersionedEvent
}

func NewInMemoryEventStreamRepository() *InMemoryEventStreamRepository {
	store := make(map[string][]VersionedEvent)
	return &InMemoryEventStreamRepository{store}
}

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

func (r *InMemoryEventStreamRepository) Get(id string, registry TypeRegistry) ([]VersionedEvent, error) {
	if events, ok := r.store[id]; ok {
		return events, nil
	}

	return nil, errors.New("Not found")
}
