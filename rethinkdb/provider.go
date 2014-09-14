package rethinkdb

import (
	"errors"
	"fmt"
	"github.com/andrewwebber/cqrs"
	rethinkdb "github.com/dancannon/gorethink"
	encoding "github.com/dancannon/gorethink/encoding"
	"log"
	"reflect"
	"time"
)

type rethinkVersionedEvent struct {
	Key       string    `gorethink:"id"`
	ID        string    `gorethink:"eventID"`
	SourceID  string    `gorethink:"sourceID"`
	Version   int       `gorethink:"version"`
	EventType string    `gorethink:"eventType"`
	Created   time.Time `gorethink:"time"`
	Event     interface{}
}

type repository struct {
	session         *rethinkdb.Session
	eventsTableName string
}

func NewRepository(options rethinkdb.ConnectOpts, eventsTableName string) (cqrs.EventStreamRepository, error) {
	session, err := rethinkdb.Connect(options)

	if err != nil {
		return nil, err
	}

	return repository{session, eventsTableName}, nil
}

func (r repository) Save(id string, events []cqrs.VersionedEvent) error {
	for _, event := range events {

		rethinkVersionedEvent := rethinkVersionedEvent{
			Key:       fmt.Sprintf("%s:%d", event.SourceID, event.Version),
			ID:        event.ID,
			SourceID:  event.SourceID,
			Version:   event.Version,
			EventType: event.EventType,
			Created:   event.Created,
			Event:     event.Event}
		log.Println("Writting ", rethinkVersionedEvent)
		rethinkdb.Table(r.eventsTableName).Insert(rethinkVersionedEvent).Run(r.session)
	}

	return nil
}

func (r repository) Get(id string, typeRegistry cqrs.TypeRegistry) ([]cqrs.VersionedEvent, error) {
	cursor, error := rethinkdb.Table(r.eventsTableName).GetAllByIndex("sourceID", id).OrderBy("version").Run(r.session)
	if error != nil {
		return nil, error
	}

	var events []cqrs.VersionedEvent
	var rawEvents []rethinkVersionedEvent
	error = cursor.All(&rawEvents)
	if error != nil {
		return nil, error
	}

	for _, raw := range rawEvents {
		eventType, ok := typeRegistry.GetEventType(raw.EventType)
		if !ok {
			log.Println("Cannot find event type", raw.EventType)
			return nil, errors.New("Cannot find event type " + raw.EventType)
		}

		eventValue := reflect.New(eventType)
		event := eventValue.Interface()
		if err := encoding.Decode(event, raw.Event); err != nil {
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
