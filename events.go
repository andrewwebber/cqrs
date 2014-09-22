package cqrs

import (
	"reflect"
	"time"
)

// VersionedEvent represents an event in the past for an aggregate
type VersionedEvent struct {
	ID        string    `json:"id"`
	SourceID  string    `json:"sourceID"`
	Version   int       `json:"version"`
	EventType string    `json:"eventType"`
	Created   time.Time `json:"time"`
	Event     interface{}
}

// VersionedEventPublisher is responsible for publishing events that have been saved to the event store\repository
type VersionedEventPublisher interface {
	PublishEvents([]VersionedEvent) error
}

// VersionedEventReceiver is responsible for receiving globally published events
type VersionedEventReceiver interface {
	ReceiveEvents(VersionedEventReceiverOptions) error
}

type VersionedEventTransactedAccept struct {
	Event                 VersionedEvent
	ProcessedSuccessfully chan bool
}

type VersionedEventReceiverOptions struct {
	EventTypeCache EventTypeCache
	Close          chan chan error
	Error          chan error
	ReceiveEvent   chan VersionedEventTransactedAccept
}

type VersionedEventDispatcher interface {
	DispatchEvent(VersionedEvent) error
}

type MapBasedVersionedEventDispatcher struct {
	registry map[reflect.Type][]VersionedEventHandler
}

type VersionedEventHandler func(VersionedEvent) error

func NewVersionedEventDispatcher() *MapBasedVersionedEventDispatcher {
	registry := make(map[reflect.Type][]VersionedEventHandler)
	return &MapBasedVersionedEventDispatcher{registry}
}

func (m *MapBasedVersionedEventDispatcher) RegisterEventHandler(event interface{}, handler VersionedEventHandler) {
	eventType := reflect.TypeOf(event)
	handlers, ok := m.registry[eventType]
	if ok {
		m.registry[eventType] = append(handlers, handler)
	} else {
		m.registry[eventType] = []VersionedEventHandler{handler}
	}
}

func (m *MapBasedVersionedEventDispatcher) DispatchEvent(event VersionedEvent) error {
	eventType := reflect.TypeOf(event.Event)
	if handlers, ok := m.registry[eventType]; ok {
		for _, handler := range handlers {
			if err := handler(event); err != nil {
				return err
			}
		}
	}

	return nil
}
