package cqrs

import uuid "github.com/satori/go.uuid"

// NewUUIDString returns a new UUID
func NewUUIDString() string {
	newUUID := uuid.NewV4()
	return newUUID.String()
}
