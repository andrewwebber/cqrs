package cqrs

import (
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
	SetSource(interface{})
	WantsToSaveSnapshot() bool
	SuggestSaveSnapshot()
}

// EventSourceBased provider a base class for aggregate times wishing to contain basis helper functionality for event sourcing
type EventSourceBased struct {
	id            string
	version       int
	events        []interface{}
	source        interface{}
	handlersCache HandlersCache
	saveSnapshot  bool
}

// NewEventSourceBased constructor
func NewEventSourceBased(source interface{}) EventSourceBased {
	return NewEventSourceBasedWithID(source, NewUUIDString())
}

// NewEventSourceBasedWithID constructor
func NewEventSourceBasedWithID(source interface{}, id string) EventSourceBased {
	return EventSourceBased{id, 0, []interface{}{}, source, createHandlersCache(source), false}
}

// Update should be called to change the state of an aggregate type
func (s *EventSourceBased) Update(versionedEvent interface{}) {
	s.CallEventHandler(versionedEvent)
	s.events = append(s.events, versionedEvent)
}

// CallEventHandler routes an event to an aggregate's event handler
func (s *EventSourceBased) CallEventHandler(event interface{}) {
	eventType := reflect.TypeOf(event)

	if handler, ok := s.handlersCache[eventType]; ok {
		handler(s.source, event)
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

// SetSource ...
func (s *EventSourceBased) SetSource(source interface{}) {
	s.source = source
}

// SetVersion sets the aggregate's Version
func (s *EventSourceBased) SetVersion(version int) {
	s.version = version
}

// Events returns a slice of newly created events since last deserialization
func (s *EventSourceBased) Events() []interface{} {
	return s.events
}

// WantsToSaveSnapshot returns whether the aggregate suggests to persist a snapshot upon the next save.
func (s *EventSourceBased) WantsToSaveSnapshot() bool {
	return s.saveSnapshot
}

// SuggestSaveSnapshot records that the aggregate suggests a save of the snapshot upon the next save.
func (s *EventSourceBased) SuggestSaveSnapshot() {
	s.saveSnapshot = true
}
