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

type CBEventSourcingRepository struct {
	bucket *couchbase.Bucket
}

func NewRepository() cqrs.EventSourcingRepository {
	c, err := couchbase.Connect("http://localhost:8091/")
	if err != nil {
		log.Fatalf("Error connecting:  %v", err)
	}

	pool, err := c.GetPool("default")
	if err != nil {
		log.Fatalf("Error getting pool:  %v", err)
	}

	bucket, err := pool.GetBucket("cqrs")
	if err != nil {
		log.Fatalf("Error getting bucket:  %v", err)
	}
	return CBEventSourcingRepository{bucket}
}

func (cber CBEventSourcingRepository) Save(source cqrs.EventSourced) error {
	id := source.ID()
	log.Println("Saving source ", id)
	currentVersion := source.Version()
	latestVersion := 0
	for i, event := range source.Events() {
		eventType := reflect.TypeOf(event)
		latestVersion = currentVersion + i
		versionedEvent := cqrs.VersionedEvent{id, latestVersion, eventType.String(), time.Now(), event}
		key := fmt.Sprintf("%s:%d", id, versionedEvent.Version)
		if error := cber.bucket.Set(key, 0, versionedEvent); error != nil {
			return error
		}
	}

	return cber.bucket.Set(id, 0, cqrs.VersionedEventSource{reflect.TypeOf(source).String(), latestVersion})
}

func (cber CBEventSourcingRepository) Get(id string, source interface{}) error {
	var cbSource cqrs.VersionedEventSource
	if error := cber.bucket.Get(id, &cbSource); error != nil {
		log.Println("Error getting event source ", id)
		return error
	}

	handlers := cqrs.GetHandlers(source)

	var events []interface{}
	for versionNumber := 0; versionNumber <= cbSource.Version; versionNumber++ {
		eventKey := fmt.Sprintf("%s:%d", id, versionNumber)
		raw := new(struct {
			SourceId  string
			Version   int
			EventType string
			Created   time.Time
			Payload   json.RawMessage
		})

		if error := cber.bucket.Get(eventKey, raw); error != nil {
			log.Println("Error getting event :", eventKey)
			return error
		}

		eventType, ok := cqrs.GetEventType(raw.EventType)
		if !ok {
			log.Println("Cannot find event type", raw.EventType)
			return nil
		}

		eventValue := reflect.New(eventType)
		event := eventValue.Interface()
		if err := json.Unmarshal(raw.Payload, event); err != nil {
			log.Println("Error deserializing event ", raw.Payload)
			return err
		}

		events = append(events, event)
		log.Println("Constructed event ", event)

		handler, ok := handlers[eventType]
		if !ok {
			errorMessage := "Cannot find handler for event type " + eventType.String()
			log.Println(errorMessage)
			return errors.New(errorMessage)
		}

		log.Println("Calling handler for ", eventType, " with value ", event, " on ", source)

		handler(source, reflect.Indirect(eventValue).Interface())
	}

	return nil
}
