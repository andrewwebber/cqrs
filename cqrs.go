package cqrs

import (
	"code.google.com/p/go-uuid/uuid"
	"log"
	"reflect"
	"strings"
	"time"
)

var (
	handlersDirectory   map[reflect.Type]HandlersCache
	eventTypes          map[string]reflect.Type
	methodHandlerPrefix = "Handle"
)

func init() {
	eventTypes = make(map[string]reflect.Type, 0)
	handlersDirectory = make(map[reflect.Type]HandlersCache, 0)
}

func GetHandlers(source interface{}) HandlersCache {
	sourceType := reflect.TypeOf(source)
	if value, ok := handlersDirectory[sourceType]; ok {
		log.Println("Found handlers for type ", sourceType.String())
		return value
	}

	panic("Cannot find handlers for type " + sourceType.String())
}

func GetEventType(eventName string) (reflect.Type, bool) {
	if eventType, ok := eventTypes[eventName]; ok {
		log.Println("Cannot find event type", eventName)
		return eventType, ok
	}

	return nil, false
}

func HandlersDirectory() *map[reflect.Type]HandlersCache {
	return &handlersDirectory
}

func EventTypes() *map[string]reflect.Type {
	return &eventTypes
}

type EventSourced interface {
	ID() string
	SetID(string)
	Version() int
	Events() []interface{}
}

type HandlersCache map[reflect.Type]func(source interface{}, event interface{})

type EventSourceBased struct {
	id      string
	version int
	events  []interface{}
	source  interface{}
}

func NewEventSourceBased(source interface{}) EventSourceBased {
	sourceType := reflect.TypeOf(source)
	var handlers HandlersCache
	if value, ok := handlersDirectory[sourceType]; ok {
		handlers = value
	} else {
		handlers = make(HandlersCache)

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

		handlersDirectory[sourceType] = handlers
	}

	return EventSourceBased{uuid.New(), 0, []interface{}{}, source}
}

func (s *EventSourceBased) Update(versionedEvent interface{}) {
	s.CallEventHandler(versionedEvent)
	s.events = append(s.events, versionedEvent)
}

func (s *EventSourceBased) CallEventHandler(versionedEvent interface{}) {
	eventType := reflect.TypeOf(versionedEvent)
	sourceType := reflect.TypeOf(s.source)
	if handlers, ok := handlersDirectory[sourceType]; ok {
		if handler, ok := handlers[eventType]; ok {
			handler(s.source, versionedEvent)
		} else {
			panic("No handler found for event type " + eventType.String())
		}
	} else {
		panic("No handlers directory entry found for source type " + sourceType.String())
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

func (s *EventSourceBased) Events() []interface{} {
	return s.events
}

type EventSourcingRepository interface {
	Save(EventSourced) error
	Get(string, interface{}) error
}

type VersionedEvent struct {
	SourceId  string
	Version   int
	EventType string
	Created   time.Time
	Payload   interface{}
}

type VersionedEventSource struct {
	Type    string
	Version int
}

func RegisterType(typeInstance interface{}) {
	rawType := reflect.TypeOf(typeInstance)
	eventTypes[rawType.String()] = rawType
	log.Println("Registered type ", rawType.String(), " with type ", rawType)
}
