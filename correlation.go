package cqrs

import (
	"time"

	"github.com/pborman/uuid"
)

const CQRSErrorEventType = "cqrs.CQRSErrorEvent"

// CQRSErrorEvent is a generic event raised within the CQRS framework
type CQRSErrorEvent struct {
	Message string
}

func DeliverCQRSError(correlationID string, err error, repo EventSourcingRepository) {
	repo.GetEventStreamRepository().SaveIntegrationEvent(VersionedEvent{
		ID:            uuid.New(),
		CorrelationID: correlationID,
		SourceID:      "",
		Version:       0,
		EventType:     CQRSErrorEventType,
		Created:       time.Now(),
		Event:         CQRSErrorEvent{Message: err.Error()}})
}
