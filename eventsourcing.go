package cqrs

import (
	"errors"
	"reflect"
	"time"
)

// EventSourcingRepository is a repository for event source based aggregates
type EventSourcingRepository interface {
	GetEventStreamRepository() EventStreamRepository
	GetTypeRegistry() TypeRegistry
	Save(EventSourced, string) ([]VersionedEvent, error)
	Get(string, EventSourced) error
	GetSnapshot(id string) (EventSourced, error)
}

// EventStreamRepository is a persistance layer for events associated with aggregates by ID
type EventStreamRepository interface {
	VersionedEventPublicationLogger
	Save(string, []VersionedEvent) error
	Get(string, int) ([]VersionedEvent, error)
	SaveSnapshot(EventSourced) error
	GetSnapshot(string) (EventSourced, error)
}

type defaultEventSourcingRepository struct {
	Registry        TypeRegistry
	EventRepository EventStreamRepository
	Publisher       VersionedEventPublisher
}

// NewRepository constructs an EventSourcingRepository
func NewRepository(eventStreamRepository EventStreamRepository, registry TypeRegistry) EventSourcingRepository {
	return NewRepositoryWithPublisher(eventStreamRepository, nil, registry)
}

// NewRepositoryWithPublisher constructs an EventSourcingRepository with a VersionedEventPublisher to dispatch events once persisted to the EventStreamRepository
func NewRepositoryWithPublisher(eventStreamRepository EventStreamRepository, publisher VersionedEventPublisher, registry TypeRegistry) EventSourcingRepository {
	return defaultEventSourcingRepository{registry, eventStreamRepository, publisher}
}

func (r defaultEventSourcingRepository) GetEventStreamRepository() EventStreamRepository {
	return r.EventRepository
}

func (r defaultEventSourcingRepository) GetTypeRegistry() TypeRegistry {
	return r.Registry
}

func (r defaultEventSourcingRepository) Save(source EventSourced, correlationID string) ([]VersionedEvent, error) {
	id := source.ID()
	if len(correlationID) == 0 {
		correlationID = "cid:" + NewUUIDString()
	}

	saveSnapshot := source.WantsToSaveSnapshot()

	currentVersion := source.Version() + 1
	var latestVersion int
	var events []VersionedEvent
	for i, event := range source.Events() {
		eventType := reflect.TypeOf(event)
		latestVersion = currentVersion + i
		versionedEvent := VersionedEvent{
			ID:            "ve:" + NewUUIDString(),
			CorrelationID: correlationID,
			SourceID:      id,
			Version:       latestVersion,
			EventType:     eventType.String(),
			Created:       time.Now().UTC(),

			Event: event}

		events = append(events, versionedEvent)

		if latestVersion%5 == 0 {
			PackageLogger().Debugf("Latest version %v", latestVersion)
			saveSnapshot = true
		}

		source.SetVersion(latestVersion)
	}

	//PackageLogger().Debugf(stringhelper.PrintJSON("defaultEventSourcingRepository.Save() Ctx Here", ctx))
	//PackageLogger().Debugf(stringhelper.PrintJSON("defaultEventSourcingRepository.Save() Events Here:", events))
	//PackageLogger().Debugf(stringhelper.PrintJSON("Source looks like: ", source))

	if len(events) > 0 {
		start := time.Now()
		if err := r.EventRepository.Save(id, events); err != nil {
			return nil, err
		}
		end := time.Now()
		PackageLogger().Debugf("defaultEventSourcingRepository.Save() - Save Events Took [%dms]", end.Sub(start)/time.Millisecond)
	}

	if saveSnapshot {
		// only save snapshot if actual aggregate events have been persisted (aka accepted)!
		saveSnap := func() {
			start := time.Now()
			PackageLogger().Debugf("Saving version %v", source.Version())
			if err := r.EventRepository.SaveSnapshot(source); err != nil {
				PackageLogger().Debugf("Unable to save snapshot: %v", err)
				// Saving the snapshot is not critical.  Continue with process...
			}
			end := time.Now()
			PackageLogger().Debugf("defaultEventSourcingRepository.Save() - Save Snapshot Took [%dms]", end.Sub(start)/time.Millisecond)

		}
		saveSnap()
	}

	if r.Publisher == nil {
		return nil, nil
	}

	start := time.Now()

	if err := r.Publisher.PublishEvents(events); err != nil {
		return nil, err
	}

	end := time.Now()
	PackageLogger().Debugf("defaultEventSourcingRepository.Save() - Publish Events Took [%dms]", end.Sub(start)/time.Millisecond)

	return events, nil
}

func (r defaultEventSourcingRepository) GetSnapshot(id string) (EventSourced, error) {
	// We don't need to error when we cant get the snapshot but lets at least record the issue.
	snapshot, err := r.EventRepository.GetSnapshot(id)
	if err != nil {
		PackageLogger().Debugf("eventsoucing: GetSnapshot(): Unable to load snapshot: [%s] %v", id, err)
		return nil, err
	}

	return snapshot, err
}

func (r defaultEventSourcingRepository) Get(id string, source EventSourced) error {

	PackageLogger().Debugf("defaultEventSourcingRepository.Get() - Get events from version %v", source.Version())

	start := time.Now()
	events, err := r.EventRepository.Get(id, source.Version()+1)
	if err != nil {
		return err
	}
	end := time.Now()
	PackageLogger().Debugf("defaultEventSourcingRepository.Get() - Got %v events took [%dms]", len(events), end.Sub(start)/time.Millisecond)

	if events == nil {
		PackageLogger().Debugf("No events to process")
		return nil
	}

	start = time.Now()

	handlers := r.Registry.GetHandlers(source)
	for _, event := range events {
		eventType := reflect.TypeOf(event.Event)
		handler, ok := handlers[eventType]
		if !ok {
			errorMessage := "Cannot find handler for event type " + event.EventType
			PackageLogger().Debugf(errorMessage)
			return errors.New(errorMessage)
		}

		handler(source, event.Event)
	}

	source.SetVersion(events[len(events)-1].Version)

	end = time.Now()
	PackageLogger().Debugf("defaultEventSourcingRepository.Get() - Get Handlers Took [%dms]", end.Sub(start)/time.Millisecond)

	return nil
}
