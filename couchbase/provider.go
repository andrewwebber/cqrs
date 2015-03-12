package couchbase

import (
	"encoding/json"
	"errors"
	"fmt"
	"log"
	"reflect"
	"sort"
	"strings"
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
	bucket   *couchbase.Bucket
	cbPrefix string
}

// NewEventStreamRepository creates new Couchbase Server based event stream repository
func NewEventStreamRepository(connectionString string, poolName string, bucketName string, prefix string) (*EventStreamRepository, error) {
	c, err := couchbase.Connect(connectionString)
	if err != nil {
		log.Println(fmt.Sprintf("Error connecting to couchbase : %v", err))
		return nil, err
	}

	pool, err := c.GetPool(poolName)
	if err != nil {
		log.Println(fmt.Sprintf("Error getting pool:  %v", err))
		return nil, err
	}

	bucket, err := pool.GetBucket(bucketName)
	if err != nil {
		log.Println(fmt.Sprintf("Error getting bucket:  %v", err))
		return nil, err
	}

	return &EventStreamRepository{bucket, prefix}, nil
}

// Save persists an event sourced object into the repository
func (r *EventStreamRepository) Save(sourceID string, events []cqrs.VersionedEvent) error {
	latestVersion := events[len(events)-1].Version
	for _, versionedEvent := range events {
		key := fmt.Sprintf("%s:%s:%d", r.cbPrefix, sourceID, versionedEvent.Version)
		if added, err := r.bucket.Add(key, 0, versionedEvent); err != nil || !added {
			return err
		}

		if err := r.SaveIntegrationEvent(versionedEvent); err != nil {
			return err
		}
	}

	cbKey := fmt.Sprintf("%s:%s", r.cbPrefix, sourceID)
	return r.bucket.Set(cbKey, 0, latestVersion)
}

// SaveIntegrationEvent persists a published integration event
func (r *EventStreamRepository) SaveIntegrationEvent(event cqrs.VersionedEvent) error {
	counter, err := r.bucket.Incr("eventstore:integration", 1, 1, 0)
	if err != nil {
		return err
	}

	key := fmt.Sprintf("eventstore:integration:%d", counter)
	if err = r.bucket.Set(key, 0, event); err != nil {
		return err
	}

	var eventsByCorrelationID map[string]cqrs.VersionedEvent
	correlationKey := "eventstore:correlation:" + event.CorrelationID
	if err := r.bucket.Get(correlationKey, &eventsByCorrelationID); err != nil {
		if IsNotFoundError(err) {
			eventsByCorrelationID = make(map[string]cqrs.VersionedEvent)
		} else {
			return err
		}
	}

	eventsByCorrelationID[event.ID] = event

	if err := r.bucket.Set(correlationKey, 0, eventsByCorrelationID); err != nil {
		return err
	}

	return nil
}

func (r *EventStreamRepository) GetIntegrationEventsByCorrelationID(correlationID string) ([]cqrs.VersionedEvent, error) {
	var eventsByCorrelationID map[string]cbVersionedEvent
	correlationKey := "eventstore:correlation:" + correlationID
	if err := r.bucket.Get(correlationKey, &eventsByCorrelationID); err != nil {
		return nil, err
	}

	typeRegistry := cqrs.NewTypeRegistry()
	var events []cqrs.VersionedEvent
	for _, raw := range eventsByCorrelationID {
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

// AllIntegrationEventsEverPublished retreives all events every persisted
func (r *EventStreamRepository) AllIntegrationEventsEverPublished() ([]cqrs.VersionedEvent, error) {
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
	cbKey := fmt.Sprintf("%s:%s", r.cbPrefix, id)
	if error := r.bucket.Get(cbKey, &version); error != nil {
		log.Println("Error getting event source ", id)
		return nil, error
	}

	var events []cqrs.VersionedEvent
	for versionNumber := 1; versionNumber <= version; versionNumber++ {
		eventKey := fmt.Sprintf("%s:%s:%d", r.cbPrefix, id, versionNumber)
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

const NOT_FOUND string = "Not found"

func IsNotFoundError(err error) bool {
	// No error?
	if err == nil {
		return false
	}

	return strings.Contains(err.Error(), NOT_FOUND)
}
