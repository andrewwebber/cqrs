package rabbit_test

import (
	"github.com/andrewwebber/cqrs"
	"github.com/andrewwebber/cqrs/rabbit"
	"log"
	"testing"
	"time"
)

type SampleEvent struct {
	Message string
}

func TestEventBus(t *testing.T) {
	bus := rabbit.NewEventBus("amqp://guest:guest@localhost:5672/", "rabbit_test", "testing.events")

	closeChannel := make(chan chan error)
	errorChannel := make(chan error)
	receiveEventChannel := make(chan cqrs.VersionedEventTransactedAccept)
	if err := bus.ReceiveEvents(cqrs.VersionedEventReceiverOptions{
		closeChannel,
		errorChannel,
		receiveEventChannel}); err != nil {
		t.Fatal(err)
	}

	log.Println("Publishing events")
	go func() {
		if err := bus.PublishEvents([]cqrs.VersionedEvent{cqrs.VersionedEvent{Event: SampleEvent{"rabbit_TestEventBus"}}}); err != nil {
			t.Fatal(err)
		}
	}()

	timeout := make(chan bool, 1)
	go func() {
		time.Sleep(5 * time.Second)
		timeout <- true
	}()
	select {
	case <-timeout:
		t.Fatal("Test timed out")
	case event := <-receiveEventChannel:
		log.Println(event.Event)
		event.ProcessedSuccessfully <- true
	case err := <-errorChannel:
		t.Fatal(err)
	}
}
