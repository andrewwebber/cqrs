package cqrs

import (
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
