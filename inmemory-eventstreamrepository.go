package cqrs

import (
	"errors"
	"log"
	"sort"
)

// InMemoryEventStreamRepository provides an inmemory event sourcing repository
type InMemoryEventStreamRepository struct {
	store             map[string][]VersionedEvent
	correlation       map[string][]VersionedEvent
	integrationEvents []VersionedEvent
}

// NewInMemoryEventStreamRepository constructor
func NewInMemoryEventStreamRepository() *InMemoryEventStreamRepository {
	store := make(map[string][]VersionedEvent)
	correlation := make(map[string][]VersionedEvent)
	return &InMemoryEventStreamRepository{store, correlation, []VersionedEvent{}}
}

// AllIntegrationEventsEverPublished returns all events ever published
func (r *InMemoryEventStreamRepository) AllIntegrationEventsEverPublished() ([]VersionedEvent, error) {
	log := r.integrationEvents
	sort.Sort(ByCreated(log))
	return log, nil
}

// SaveIntegrationEvent persists an integration event
func (r *InMemoryEventStreamRepository) SaveIntegrationEvent(event VersionedEvent) error {
	r.integrationEvents = append(r.integrationEvents, event)
	events := r.correlation[event.CorrelationID]
	events = append(events, event)
	r.correlation[event.CorrelationID] = events

	log.Println("Saving int event ", event.CorrelationID, events)

	return nil
}

// GetIntegrationEventsByCorrelationID returns all integration events with a matching correlationID
func (r *InMemoryEventStreamRepository) GetIntegrationEventsByCorrelationID(correlationID string) ([]VersionedEvent, error) {
	events, _ := r.correlation[correlationID]
	return events, nil
}

// Save persists an event sourced object into the repository
func (r *InMemoryEventStreamRepository) Save(id string, newEvents []VersionedEvent) error {
	for _, event := range newEvents {
		if err := r.SaveIntegrationEvent(event); err != nil {
			return err
		}
	}

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
