package cqrs

import (
	"time"
)

// CQRSErrorEventType ...
const CQRSErrorEventType = "cqrs.ErrorEvent"

// ErrorEvent is a generic event raised within the CQRS framework
type ErrorEvent struct {
	Message string
}

// DeliverCQRSError will deliver a CQRS error
func DeliverCQRSError(correlationID string, err error, repo EventSourcingRepository) {
	err = repo.GetEventStreamRepository().SaveIntegrationEvent(VersionedEvent{
		ID:            "ve:" + NewUUIDString(),
		CorrelationID: correlationID,
		SourceID:      "",
		Version:       0,
		EventType:     CQRSErrorEventType,
		Created:       time.Now(),

		Event: ErrorEvent{Message: err.Error()}})

	if err != nil {
		PackageLogger().Debugf("ERROR saving integration event: %v\n", err)
	}
}
