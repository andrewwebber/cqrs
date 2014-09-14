package cqrs

import (
	"code.google.com/p/go-uuid/uuid"
	"errors"
	"log"
	"reflect"
	"strings"
	"time"
)

var methodHandlerPrefix = "Handle"

// TypeRegistry providers a helper registry for mapping event types and handlers after performance json serializaton
type TypeRegistry interface {
	GetHandlers(interface{}) HandlersCache
	GetEventType(string) (reflect.Type, bool)
	RegisterAggregate(aggregate interface{}, events ...interface{})
}

type defaultTypeRegistry struct {
	handlersDirectory map[reflect.Type]HandlersCache
	eventTypes        map[string]reflect.Type
}

func newTypeRegistry() defaultTypeRegistry {
	handlersDirectory := make(map[reflect.Type]HandlersCache, 0)
	eventTypes := make(map[string]reflect.Type, 0)

	return defaultTypeRegistry{handlersDirectory, eventTypes}
}

func (r defaultTypeRegistry) GetHandlers(source interface{}) HandlersCache {
	sourceType := reflect.TypeOf(source)
	var handlers HandlersCache
	if value, ok := r.handlersDirectory[sourceType]; ok {
		handlers = value
	} else {
		handlers = createHandlersCache(source)

		r.handlersDirectory[sourceType] = handlers
	}

	return handlers
}

func (r defaultTypeRegistry) GetEventType(eventType string) (reflect.Type, bool) {
	if eventTypeValue, ok := r.eventTypes[eventType]; ok {
		return eventTypeValue, ok
	}

	return nil, false
}

func createHandlersCache(source interface{}) HandlersCache {
	sourceType := reflect.TypeOf(source)
	handlers := make(HandlersCache)

	methodCount := sourceType.NumMethod()
	for i := 0; i < methodCount; i++ {
		method := sourceType.Method(i)

		if strings.HasPrefix(method.Name, methodHandlerPrefix) {
			//   func (source *MySource) HandleMyEvent(e *MyEvent).
			if method.Type.NumIn() == 2 {
				eventType := method.Type.In(1)
				handler := func(source interface{}, event interface{}) {
					sourceValue := reflect.ValueOf(source)
					eventValue := reflect.ValueOf(event)

					method.Func.Call([]reflect.Value{sourceValue, eventValue})
				}

				handlers[eventType] = handler
			}
		}
	}

	return handlers
}

// EventSourced providers an interface for event sourced aggregate types
type EventSourced interface {
	ID() string
	SetID(string)
	Version() int
	SetVersion(int)
	Events() []interface{}
	CallEventHandler(versionedEvent interface{})
}

// HandlersCache is a map of types to functions that will be used to route event sourcing events
type HandlersCache map[reflect.Type]func(source interface{}, event interface{})

// EventSourceBased provider a base class for aggregate times wishing to contain basis helper functionality for event sourcing
type EventSourceBased struct {
	id            string
	version       int
	events        []interface{}
	source        interface{}
	handlersCache HandlersCache
}

// NewEventSourceBased constructor
func NewEventSourceBased(source interface{}) EventSourceBased {
	return NewEventSourceBasedWithID(source, uuid.New())
}

// NewEventSourceBasedWithID constructor
func NewEventSourceBasedWithID(source interface{}, id string) EventSourceBased {
	return EventSourceBased{id, 0, []interface{}{}, source, createHandlersCache(source)}
}

// Update should be called to change the state of an aggregate type
func (s *EventSourceBased) Update(versionedEvent interface{}) {
	s.CallEventHandler(versionedEvent)
	s.events = append(s.events, versionedEvent)
}

// CallEventHandler routes an event to an aggregate's event handler
func (s *EventSourceBased) CallEventHandler(versionedEvent interface{}) {
	eventType := reflect.TypeOf(versionedEvent)

	if handler, ok := s.handlersCache[eventType]; ok {
		handler(s.source, versionedEvent)
	} else {
		panic("No handler found for event type " + eventType.String())
	}
}

// ID provider the aggregate's ID
func (s *EventSourceBased) ID() string {
	return s.id
}

// SetID sets the aggregate's ID
func (s *EventSourceBased) SetID(id string) {
	s.id = id
}

// Version provider the aggregate's Version
func (s *EventSourceBased) Version() int {
	return s.version
}

// SetVersion sets the aggregate's Version
func (s *EventSourceBased) SetVersion(version int) {
	s.version = version
}

// Events returns a slice of newly created events since last deserialization
func (s *EventSourceBased) Events() []interface{} {
	return s.events
}

// VersionedEvent represents an event in the past for an aggregate
type VersionedEvent struct {
	ID        string    `json:"id"`
	SourceID  string    `json:"sourceID"`
	Version   int       `json:"version"`
	EventType string    `json:"eventType"`
	Created   time.Time `json:"time"`
	Event     interface{}
}

// EventSourcingRepository is a repository for event source based aggregates
type EventSourcingRepository interface {
	TypeRegistry
	Save(EventSourced) error
	Get(string, EventSourced) error
}

// EventStreamRepository is a persistance layer for events associated with aggregates by ID
type EventStreamRepository interface {
	Save(string, []VersionedEvent) error
	Get(string, TypeRegistry) ([]VersionedEvent, error)
}

type defaultEventSourcingRepository struct {
	defaultTypeRegistry
	EventRepository EventStreamRepository
}

// NewRepository is a constructor for the EventSourcingRepository
func NewRepository(eventStreamRepository EventStreamRepository) EventSourcingRepository {
	return defaultEventSourcingRepository{newTypeRegistry(), eventStreamRepository}
}

func (r defaultEventSourcingRepository) Save(source EventSourced) error {
	id := source.ID()
	currentVersion := source.Version() + 1
	latestVersion := 0
	var events []VersionedEvent
	for i, event := range source.Events() {
		eventType := reflect.TypeOf(event)
		latestVersion = currentVersion + i
		versionedEvent := VersionedEvent{
			ID:        uuid.New(),
			SourceID:  id,
			Version:   latestVersion,
			EventType: eventType.String(),
			Created:   time.Now(),
			Event:     event}

		events = append(events, versionedEvent)
	}

	if error := r.EventRepository.Save(id, events); error != nil {
		return error
	}

	return nil
}

func (r defaultEventSourcingRepository) Get(id string, source EventSourced) error {
	events, error := r.EventRepository.Get(id, r)
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

func (r defaultTypeRegistry) RegisterAggregate(aggregate interface{}, events ...interface{}) {
	r.RegisterType(aggregate)

	for _, event := range events {
		r.RegisterType(event)
	}
}

func (r defaultTypeRegistry) RegisterType(source interface{}) {
	rawType := reflect.TypeOf(source)
	r.eventTypes[rawType.String()] = rawType
}
