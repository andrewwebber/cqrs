package rabbit_test

import (
	"errors"
	"reflect"
	"testing"
	"time"

	"github.com/andrewwebber/cqrs"
	"github.com/andrewwebber/cqrs/rabbit"
)

type SampleEvent struct {
	Message string
}

// Simple test for publishing and received versioned events using rabbitmq
func TestEventBus(t *testing.T) {
	if testing.Short() {
		t.SkipNow()
	}

	// Create a new event bus
	connectionString := func() (string, error) { return "amqp://guest:guest@localhost:5672/", nil }
	bus := rabbit.NewEventBus(connectionString, "rabbit_testevents", "testing.events")

	// Register types
	eventType := reflect.TypeOf(SampleEvent{})
	eventTypeCache := cqrs.NewTypeRegistry()
	eventTypeCache.RegisterType(SampleEvent{})

	// Create communication channels
	//
	// for closing the queue listener,
	closeChannel := make(chan chan error)
	// receiving errors from the listener thread (go routine)
	errorChannel := make(chan error)
	// and receiving events from the queue
	receiveEventChannel := make(chan cqrs.VersionedEventTransactedAccept)
	eventHandler := func(event cqrs.VersionedEvent) error {
		accepted := make(chan bool)
		receiveEventChannel <- cqrs.VersionedEventTransactedAccept{Event: event, ProcessedSuccessfully: accepted}
		if <-accepted {
			cqrs.PackageLogger().Debugf("event process successfully")
			return nil
		}

		t.Fatal("Unsuccessful")
		return errors.New("Unsuccessful")
	}
	// Start receiving events by passing these channels to the worker thread (go routine)
	if err := bus.ReceiveEvents(cqrs.VersionedEventReceiverOptions{TypeRegistry: eventTypeCache, Close: closeChannel, Error: errorChannel, ReceiveEvent: eventHandler, Exclusive: false, ListenerCount: 1}); err != nil {
		t.Fatal(err)
	}

	// Publish a simple event to the exchange http://www.rabbitmq.com/tutorials/tutorial-three-python.html
	t.Log("Publishing events")
	go func() {
		if err := bus.PublishEvents([]cqrs.VersionedEvent{{
			EventType: eventType.String(),
			Event:     SampleEvent{"rabbit_TestEventBus"}}}); err != nil {
			t.Fatal(err)
		}
		t.Log("Events published")
	}()

	// If we dont receive a message within 5 seconds this test is a failure. Use a channel to signal the timeout
	timeout := make(chan bool, 1)
	go func() {
		time.Sleep(10 * time.Second)
		timeout <- true
	}()

	// Wait on multiple channels using the select control flow.
	select {
	// Test timeout
	case <-timeout:
		t.Fatal("Test timed out")
		// Version event received channel receives a result with a channel to respond to, signifying successful processing of the message.
		// This should eventually call an event handler. See cqrs.NewVersionedEventDispatcher()
	case event := <-receiveEventChannel:
		sampleEvent := event.Event.Event.(SampleEvent)
		t.Log(sampleEvent.Message)
		event.ProcessedSuccessfully <- true
		// Receiving on this channel signifys an error has occured work processor side
	case err := <-errorChannel:
		t.Fatal(err)
	}

	closeAck := make(chan error)
	closeChannel <- closeAck
	<-closeAck
}
