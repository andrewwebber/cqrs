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

type EventsCache map[string]reflect.Type

type TypeRegistry interface {
	GetHandlers(interface{}) HandlersCache
	GetEventType(string) (reflect.Type, bool)
	RegisterAggregate(aggregate interface{}, events ...interface{})
}

type DefaultTypeRegistry struct {
	handlersDirectory map[reflect.Type]HandlersCache
	eventTypes        map[string]reflect.Type
}

func NewTypeRegistry() DefaultTypeRegistry {
	handlersDirectory := make(map[reflect.Type]HandlersCache, 0)
	eventTypes := make(map[string]reflect.Type, 0)

	return DefaultTypeRegistry{handlersDirectory, eventTypes}
}

func (r DefaultTypeRegistry) GetHandlers(source interface{}) HandlersCache {
	sourceType := reflect.TypeOf(source)
	var handlers HandlersCache
	if value, ok := r.handlersDirectory[sourceType]; ok {
		handlers = value
	} else {
		handlers = CreateHandlersCache(source)

		r.handlersDirectory[sourceType] = handlers
	}

	return handlers
}

func (r DefaultTypeRegistry) GetEventType(eventType string) (reflect.Type, bool) {
	if eventTypeValue, ok := r.eventTypes[eventType]; ok {
		return eventTypeValue, ok
	}

	return nil, false
}

func CreateHandlersCache(source interface{}) HandlersCache {
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

type EventSourced interface {
	ID() string
	SetID(string)
	Version() int
	SetVersion(int)
	Events() []interface{}
	CallEventHandler(versionedEvent interface{})
}

type HandlersCache map[reflect.Type]func(source interface{}, event interface{})

type EventSourceBased struct {
	id            string
	version       int
	events        []interface{}
	source        interface{}
	handlersCache HandlersCache
}

func NewEventSourceBased(source interface{}) EventSourceBased {
	return NewEventSourceBasedWithID(source, uuid.New())
}

func NewEventSourceBasedWithID(source interface{}, id string) EventSourceBased {
	return EventSourceBased{id, 0, []interface{}{}, source, CreateHandlersCache(source)}
}

func (s *EventSourceBased) Update(versionedEvent interface{}) {
	s.CallEventHandler(versionedEvent)
	s.events = append(s.events, versionedEvent)
}

func (s *EventSourceBased) CallEventHandler(versionedEvent interface{}) {
	eventType := reflect.TypeOf(versionedEvent)

	if handler, ok := s.handlersCache[eventType]; ok {
		handler(s.source, versionedEvent)
	} else {
		panic("No handler found for event type " + eventType.String())
	}
}

func (s *EventSourceBased) ID() string {
	return s.id
}

func (s *EventSourceBased) SetID(id string) {
	s.id = id
}

func (s *EventSourceBased) Version() int {
	return s.version
}

func (s *EventSourceBased) SetVersion(version int) {
	s.version = version
}

func (s *EventSourceBased) Events() []interface{} {
	return s.events
}

type VersionedEvent struct {
	ID        string    `json:"id"`
	SourceID  string    `json:"sourceID"`
	Version   int       `json:"version"`
	EventType string    `json:"eventType"`
	Created   time.Time `json:"time"`
	Event     interface{}
}

type EventSourcingRepository interface {
	Save(EventSourced) error
	Get(string, EventSourced) error
}

type EventStreamRepository interface {
	Save(string, []VersionedEvent) error
	Get(string, TypeRegistry) ([]VersionedEvent, error)
}

type DefaultEventSourcingRepository struct {
	DefaultTypeRegistry
	EventRepository EventStreamRepository
}

func NewRepository(eventStreamRepository EventStreamRepository) DefaultEventSourcingRepository {
	return DefaultEventSourcingRepository{NewTypeRegistry(), eventStreamRepository}
}

func (r DefaultEventSourcingRepository) Save(source EventSourced) error {
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

func (r DefaultEventSourcingRepository) Get(id string, source EventSourced) error {
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

func (r DefaultTypeRegistry) RegisterAggregate(aggregate interface{}, events ...interface{}) {
	r.RegisterType(aggregate)

	for _, event := range events {
		r.RegisterType(event)
	}
}

func (r DefaultTypeRegistry) RegisterType(source interface{}) {
	rawType := reflect.TypeOf(source)
	r.eventTypes[rawType.String()] = rawType
}
