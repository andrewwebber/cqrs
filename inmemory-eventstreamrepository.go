package cqrs

import (
	"errors"
	"sort"
)

// InMemoryEventStreamRepository provides an inmemory event sourcing repository
type InMemoryEventStreamRepository struct {
	store             map[string][]VersionedEvent
	integrationEvents []VersionedEvent
}

// NewInMemoryEventStreamRepository constructor
func NewInMemoryEventStreamRepository() *InMemoryEventStreamRepository {
	store := make(map[string][]VersionedEvent)
	return &InMemoryEventStreamRepository{store, []VersionedEvent{}}
}

// AllEventsEverPublished returns all events ever published
func (r *InMemoryEventStreamRepository) AllEventsEverPublished() ([]VersionedEvent, error) {
	var log []VersionedEvent
	for key := range r.store {
		for _, event := range r.store[key] {
			log = append(log, event)
		}
	}

	sort.Sort(ByCreated(log))

	return log, nil
}

// SaveIntegrationEvent persists an integration event
func (r *InMemoryEventStreamRepository) SaveIntegrationEvent(event VersionedEvent) error {
	r.integrationEvents = append(r.integrationEvents, event)
	return nil
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
func (r *InMemoryEventStreamRepository) Get(id string) ([]VersionedEvent, error) {
	if events, ok := r.store[id]; ok {
		return events, nil
	}

	return nil, errors.New("not found")
}
