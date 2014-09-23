package cqrs

import (
	"code.google.com/p/go-uuid/uuid"
	"reflect"
)

// EventSourced providers an interface for event sourced aggregate types
type EventSourced interface {
	ID() string
	SetID(string)
	Version() int
	SetVersion(int)
	Events() []interface{}
	CallEventHandler(event interface{})
}

// EventSource provides a base class for aggregates wishing to contain basic helper functionality for event sourcing
type EventSource struct {
	id            string
	version       int
	events        []interface{}
	source        interface{}
	handlersCache HandlersCache
}

// NewEventSource constructor
func NewEventSource(source interface{}) EventSource {
	return NewEventSourceWithID(source, uuid.New())
}

// NewEventSourceWithID constructor
func NewEventSourceWithID(source interface{}, id string) EventSource {
	return EventSource{id, 0, []interface{}{}, source, createHandlersCache(source)}
}

// Update should be called to change the state of an aggregate type
func (s *EventSource) Update(versionedEvent interface{}) {
	s.CallEventHandler(versionedEvent)
	s.events = append(s.events, versionedEvent)
}

// CallEventHandler routes an event to an aggregate's event handler
func (s *EventSource) CallEventHandler(event interface{}) {
	eventType := reflect.TypeOf(event)

	if handler, ok := s.handlersCache[eventType]; ok {
		handler(s.source, event)
	} else {
		panic("No handler found for event type " + eventType.String())
	}
}

// ID provider the aggregate's ID
func (s *EventSource) ID() string {
	return s.id
}

// SetID sets the aggregate's ID
func (s *EventSource) SetID(id string) {
	s.id = id
}

// Version provider the aggregate's Version
func (s *EventSource) Version() int {
	return s.version
}

// SetVersion sets the aggregate's Version
func (s *EventSource) SetVersion(version int) {
	s.version = version
}

// Events returns a slice of newly created events since last deserialization
func (s *EventSource) Events() []interface{} {
	return s.events
}
