package couchbase

import (
	"encoding/json"
	"errors"
	"fmt"
	"github.com/andrewwebber/cqrs"
	"github.com/couchbaselabs/go-couchbase"
	"log"
	"reflect"
	"time"
)

type cbVersionedEvent struct {
	ID        string    `json:"id"`
	SourceID  string    `json:"sourceID"`
	Version   int       `json:"version"`
	EventType string    `json:"eventType"`
	Created   time.Time `json:"time"`
	Event     json.RawMessage
}

// Repository : a Couchbase Server event stream repository
type repository struct {
	bucket *couchbase.Bucket
}

// NewEventStreamRepository creates new Couchbase Server based event stream repository
func NewEventStreamRepository(connectionString string) (cqrs.EventStreamRepository, error) {
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

	return repository{bucket}, nil
}

// Save persists an event sourced object into the repository
func (r repository) Save(sourceID string, events []cqrs.VersionedEvent) error {
	latestVersion := events[len(events)-1].Version
	for _, versionedEvent := range events {
		key := fmt.Sprintf("%s:%d", sourceID, versionedEvent.Version)
		if error := r.bucket.Set(key, 0, versionedEvent); error != nil {
			return error
		}
	}

	return r.bucket.Set(sourceID, 0, latestVersion)
}

// Get retrieves an event sourced object by ID
func (r repository) Get(id string, typeRegistry cqrs.TypeRegistry) ([]cqrs.VersionedEvent, error) {
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

		eventType, ok := typeRegistry.GetEventType(raw.EventType)
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
