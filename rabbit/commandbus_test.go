package rabbit_test

import (
	"log"
	"reflect"
	"testing"
	"time"

	"github.com/andrewwebber/cqrs"
	"github.com/andrewwebber/cqrs/rabbit"
)

type SampleCommand struct {
	Message string
}

// Simple test for publishing and received versioned events using rabbitmq
func TestCommandBus(t *testing.T) {
	// Create a new event bus
	bus := rabbit.NewCommandBus("amqp://guest:guest@localhost:5672/", "rabbit_testcommands", "testing.commands")

	// Register types
	commandType := reflect.TypeOf(SampleCommand{})
	commandTypeCache := cqrs.NewTypeRegistry()
	commandTypeCache.RegisterType(SampleCommand{})

	// Create communication channels
	//
	// for closing the queue listener,
	closeChannel := make(chan chan error)
	// receiving errors from the listener thread (go routine)
	errorChannel := make(chan error)
	// and receiving commands from the queue
	receiveCommandChannel := make(chan cqrs.CommandTransactedAccept)
	// Start receiving events by passing these channels to the worker thread (go routine)
	if err := bus.ReceiveCommands(cqrs.CommandReceiverOptions{commandTypeCache, closeChannel, errorChannel, receiveCommandChannel, false}); err != nil {
		t.Fatal(err)
	}

	// Publish a simple event to the exchange http://www.rabbitmq.com/tutorials/tutorial-three-python.html
	log.Println("Publishing Commands")
	go func() {
		if err := bus.PublishCommands([]cqrs.Command{cqrs.Command{
			CommandType: commandType.String(),
			Body:        SampleCommand{"rabbit_TestCommandBus"}}}); err != nil {
			t.Fatal(err)
		}
	}()

	// If we dont receive a message within 5 seconds this test is a failure. Use a channel to signal the timeout
	timeout := make(chan bool, 1)
	go func() {
		time.Sleep(5 * time.Second)
		timeout <- true
	}()

	// Wait on multiple channels using the select control flow.
	select {
	// Test timeout
	case <-timeout:
		t.Fatal("Test timed out")
		// Version event received channel receives a result with a channel to respond to, signifying successful processing of the message.
		// This should eventually call an event handler. See cqrs.NewVersionedEventDispatcher()
	case command := <-receiveCommandChannel:
		sampleCommand := command.Command.Body.(SampleCommand)
		log.Println(sampleCommand.Message)
		command.ProcessedSuccessfully <- true
		// Receiving on this channel signifys an error has occured work processor side
	case err := <-errorChannel:
		t.Fatal(err)
	}
}
