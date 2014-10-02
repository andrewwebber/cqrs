package couchbase

import (
	"encoding/json"
	"errors"
	"fmt"
	"log"
	"reflect"
	"sort"
	"time"

	"github.com/andrewwebber/cqrs"
	"github.com/couchbaselabs/go-couchbase"
)

type cbVersionedEvent struct {
	ID            string    `json:"id"`
	CorrelationID string    `json:"correlationID"`
	SourceID      string    `json:"sourceID"`
	Version       int       `json:"version"`
	EventType     string    `json:"eventType"`
	Created       time.Time `json:"time"`
	Event         json.RawMessage
}

// EventStreamRepository : a Couchbase Server event stream repository
type EventStreamRepository struct {
	bucket *couchbase.Bucket
}

// NewEventStreamRepository creates new Couchbase Server based event stream repository
func NewEventStreamRepository(connectionString string) (*EventStreamRepository, error) {
	c, err := couchbase.Connect(connectionString)
	if err != nil {
		log.Println(fmt.Sprintf("Error connecting to couchbase : %v", err))
		return nil, err
	}

	pool, err := c.GetPool("default")
	if err != nil {
		log.Println(fmt.Sprintf("Error getting pool:  %v", err))
		return nil, err
	}

	bucket, err := pool.GetBucket("cqrs")
	if err != nil {
		log.Println(fmt.Sprintf("Error getting bucket:  %v", err))
		return nil, err
	}

	return &EventStreamRepository{bucket}, nil
}

// Save persists an event sourced object into the repository
func (r *EventStreamRepository) Save(sourceID string, events []cqrs.VersionedEvent) error {
	latestVersion := events[len(events)-1].Version
	for _, versionedEvent := range events {
		key := fmt.Sprintf("%s:%d", sourceID, versionedEvent.Version)
		if added, error := r.bucket.Add(key, 0, versionedEvent); error != nil || !added {
			return error
		}
	}

	return r.bucket.Set(sourceID, 0, latestVersion)
}

// SaveIntegrationEvent persists a published integration event
func (r *EventStreamRepository) SaveIntegrationEvent(event cqrs.VersionedEvent) error {
	counter, err := r.bucket.Incr("integration", 1, 1, 0)
	if err != nil {
		return err
	}

	key := fmt.Sprintf("integration::%d", counter)
	if err = r.bucket.Set(key, 0, event); err != nil {
		return err
	}

	return nil
}

// AllEventsEverPublished retreives all events every persisted
func (r *EventStreamRepository) AllEventsEverPublished() ([]cqrs.VersionedEvent, error) {
	var counter int
	if err := r.bucket.Get("integration", &counter); err != nil {
		return nil, err
	}

	var result []cqrs.VersionedEvent
	for i := 0; i < counter; i++ {
		key := fmt.Sprintf("integration::%d", i)
		events, err := r.Get(key)
		if err != nil {
			return nil, err
		}

		for _, event := range events {
			result = append(result, event)
		}
	}

	sort.Sort(cqrs.ByCreated(result))

	return result, nil
}

// Get retrieves events assoicated with an event sourced object by ID
func (r *EventStreamRepository) Get(id string) ([]cqrs.VersionedEvent, error) {
	var version int
	if error := r.bucket.Get(id, &version); error != nil {
		log.Println("Error getting event source ", id)
		return nil, error
	}

	var events []cqrs.VersionedEvent
	for versionNumber := 1; versionNumber <= version; versionNumber++ {
		eventKey := fmt.Sprintf("%s:%d", id, versionNumber)
		raw := new(cbVersionedEvent)

		if error := r.bucket.Get(eventKey, raw); error != nil {
			log.Println("Error getting event :", eventKey)
			return nil, error
		}

		typeRegistry := cqrs.NewTypeRegistry()

		eventType, ok := typeRegistry.GetTypeByName(raw.EventType)
		if !ok {
			log.Println("Cannot find event type", raw.EventType)
			return nil, errors.New("Cannot find event type " + raw.EventType)
		}

		eventValue := reflect.New(eventType)
		event := eventValue.Interface()
		if err := json.Unmarshal(raw.Event, event); err != nil {
			log.Println("Error deserializing event ", raw.Event)
			return nil, err
		}

		versionedEvent := cqrs.VersionedEvent{
			ID:        raw.ID,
			SourceID:  raw.SourceID,
			Version:   raw.Version,
			EventType: raw.EventType,
			Created:   raw.Created,
			Event:     reflect.Indirect(eventValue).Interface()}

		events = append(events, versionedEvent)
	}

	return events, nil
}
