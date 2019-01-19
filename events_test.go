package cqrs_test

import (
	"testing"

	"github.com/andrewwebber/cqrs"
)

type SampleMessageReceivedEvent struct {
	Message string
}

func TestDefaultVersionedEventDispatcher(t *testing.T) {
	dispatcher := cqrs.NewVersionedEventDispatcher()
	success := false
	dispatcher.RegisterEventHandler(SampleMessageReceivedEvent{}, func(event cqrs.VersionedEvent) error {
		cqrs.PackageLogger().Debugf("Received Message : ", event.Event.(SampleMessageReceivedEvent).Message)
		success = true
		return nil
	})

	err := dispatcher.DispatchEvent(cqrs.VersionedEvent{Event: SampleMessageReceivedEvent{"Hello world"}})
	if err != nil {
		t.Fatal(err)
	}
	if !success {
		t.Fatal("Expected success")
	}
}
