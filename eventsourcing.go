package cqrs

import (
	"code.google.com/p/go-uuid/uuid"
	"errors"
	"log"
	"reflect"
	"time"
)

// EventSourcingRepository is a repository for event source based aggregates
type EventSourcingRepository interface {
	TypeRegistry
	Save(EventSourced, string) error
	Get(string, EventSourced) error
}

// EventStreamRepository is a persistance layer for events associated with aggregates by ID
type EventStreamRepository interface {
	Save(string, []VersionedEvent) error
	Get(string) ([]VersionedEvent, error)
}

type defaultEventSourcingRepository struct {
	*defaultTypeRegistry
	EventRepository EventStreamRepository
	Publisher       VersionedEventPublisher
}

// NewRepository constructs an EventSourcingRepository
func NewRepository(eventStreamRepository EventStreamRepository) EventSourcingRepository {
	return NewRepositoryWithPublisher(eventStreamRepository, nil)
}

// NewRepositoryWithPublisher constructs an EventSourcingRepository with a VersionedEventPublisher to dispatch events once persisted to the EventStreamRepository
func NewRepositoryWithPublisher(eventStreamRepository EventStreamRepository, publisher VersionedEventPublisher) EventSourcingRepository {
	return defaultEventSourcingRepository{newTypeRegistry(), eventStreamRepository, publisher}
}

func (r defaultEventSourcingRepository) Save(source EventSourced, correlationID string) error {
	id := source.ID()
	if len(correlationID) == 0 {
		correlationID = uuid.New()
	}

	currentVersion := source.Version() + 1
	latestVersion := 0
	var events []VersionedEvent
	for i, event := range source.Events() {
		eventType := reflect.TypeOf(event)
		latestVersion = currentVersion + i
		versionedEvent := VersionedEvent{
			ID:            uuid.New(),
			CorrelationID: correlationID,
			SourceID:      id,
			Version:       latestVersion,
			EventType:     eventType.String(),
			Created:       time.Now(),
			Event:         event}

		events = append(events, versionedEvent)
	}

	if error := r.EventRepository.Save(id, events); error != nil {
		return error
	}

	if r.Publisher == nil {
		return nil
	}

	if error := r.Publisher.PublishEvents(events); error != nil {
		return error
	}

	return nil
}

func (r defaultEventSourcingRepository) Get(id string, source EventSourced) error {
	events, error := r.EventRepository.Get(id)
	if error != nil {
		return error
	}

	handlers := r.GetHandlers(source)
	for _, event := range events {
		eventType := reflect.TypeOf(event.Event)
		handler, ok := handlers[eventType]
		if !ok {
			errorMessage := "Cannot find handler for event type " + event.EventType
			log.Println(errorMessage)
			return errors.New(errorMessage)
		}

		handler(source, event.Event)
	}

	source.SetVersion(events[len(events)-1].Version)
	return nil
}
