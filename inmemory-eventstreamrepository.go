package cqrs

import (
	"errors"
	"sort"
	"sync"
)

// InMemoryEventStreamRepository provides an inmemory event sourcing repository
type InMemoryEventStreamRepository struct {
	lock              sync.Mutex
	store             map[string][]VersionedEvent
	correlation       map[string][]VersionedEvent
	integrationEvents []VersionedEvent
	eventSourcedStore map[string]EventSourced
}

// NewInMemoryEventStreamRepository constructor
func NewInMemoryEventStreamRepository() *InMemoryEventStreamRepository {
	store := make(map[string][]VersionedEvent)
	correlation := make(map[string][]VersionedEvent)
	eventSourcedStore := make(map[string]EventSourced)
	return &InMemoryEventStreamRepository{sync.Mutex{}, store, correlation, []VersionedEvent{}, eventSourcedStore}
}

// AllIntegrationEventsEverPublished returns all events ever published
func (r *InMemoryEventStreamRepository) AllIntegrationEventsEverPublished() ([]VersionedEvent, error) {
	r.lock.Lock()
	defer r.lock.Unlock()
	log := r.integrationEvents
	sort.Sort(ByCreated(log))
	return log, nil
}

// SaveIntegrationEvent persists an integration event
func (r *InMemoryEventStreamRepository) SaveIntegrationEvent(event VersionedEvent) error {
	r.lock.Lock()
	defer r.lock.Unlock()

	r.integrationEvents = append(r.integrationEvents, event)
	events := r.correlation[event.CorrelationID]
	events = append(events, event)
	r.correlation[event.CorrelationID] = events

	PackageLogger().Debugf("Saving SaveIntegrationEvent event ", event.CorrelationID, events)

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

	r.lock.Lock()
	defer r.lock.Unlock()

	if events, ok := r.store[id]; ok {
		events = append(events, newEvents...)

		r.store[id] = events
		return nil
	}

	r.store[id] = newEvents
	return nil
}

// Get retrieves events assoicated with an event sourced object by ID
func (r *InMemoryEventStreamRepository) Get(id string, fromVersion int) ([]VersionedEvent, error) {
	r.lock.Lock()
	defer r.lock.Unlock()

	var events []VersionedEvent

	if allEvents, ok := r.store[id]; ok {
		for _, event := range allEvents {
			if event.Version < fromVersion {
				continue
			}

			events = append(events, event)
		}

		return events, nil
	}

	return nil, errors.New("not found")
}

// SaveSnapshot ...
func (r *InMemoryEventStreamRepository) SaveSnapshot(eventsourced EventSourced) error {
	r.eventSourcedStore[eventsourced.ID()] = eventsourced
	return nil
}

// GetSnapshot ...
func (r *InMemoryEventStreamRepository) GetSnapshot(id string) (EventSourced, error) {
	value, ok := r.eventSourcedStore[id]

	if !ok {
		return nil, errors.New("not found")
	}

	return value, nil
}
