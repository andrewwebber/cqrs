package cqrs_test

import (
	"github.com/andrewwebber/cqrs"
	"log"
	"testing"
)

type SampleMessageReceivedEvent struct {
	Message string
}

func TestDefaultVersionedEventDispatcher(t *testing.T) {
	dispatcher := cqrs.NewVersionedEventDispatcher()
	success := false
	dispatcher.RegisterEventHandler(SampleMessageReceivedEvent{}, func(event cqrs.VersionedEvent) error {
		log.Println("Received Message : ", event.Event.(SampleMessageReceivedEvent).Message)
		success = true
		return nil
	})

	dispatcher.DispatchEvent(cqrs.VersionedEvent{Event: SampleMessageReceivedEvent{"Hello world"}})
	if !success {
		t.Fatal("Expected success")
	}
}
