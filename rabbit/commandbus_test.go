package rabbit_test

import (
	"errors"
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
	connectionString := func() (string, error) { return "amqp://guest:guest@localhost:5672/", nil }
	bus := rabbit.NewCommandBus(connectionString, "rabbit_testcommands", "testing.commands")

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
	commandHandler := func(command cqrs.Command) error {
		accepted := make(chan bool)
		receiveCommandChannel <- cqrs.CommandTransactedAccept{Command: command, ProcessedSuccessfully: accepted}
		t.Log("Send Message")
		if <-accepted {
			return nil
		}

		t.Fatal("Unsuccessful")
		return errors.New("Unsuccessful")
	}
	// Start receiving events by passing these channels to the worker thread (go routine)
	if err := bus.ReceiveCommands(cqrs.CommandReceiverOptions{TypeRegistry: commandTypeCache, Close: closeChannel, Error: errorChannel, ReceiveCommand: commandHandler, Exclusive: false, ListenerCount: 1}); err != nil {
		t.Fatal(err)
	}

	// Publish a simple event to the exchange http://www.rabbitmq.com/tutorials/tutorial-three-python.html
	t.Log("Publishing Commands")
	go func() {
		if err := bus.PublishCommands([]cqrs.Command{{
			CommandType: commandType.String(),
			Body:        SampleCommand{"rabbit_TestCommandBus"}}}); err != nil {
			t.Fatal(err)
		}

		t.Log("Command published")
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
	case command := <-receiveCommandChannel:
		sampleCommand := command.Command.Body.(SampleCommand)
		t.Log(sampleCommand.Message)
		command.ProcessedSuccessfully <- true
		// Receiving on this channel signifys an error has occured work processor side
	case err := <-errorChannel:
		t.Fatal(err)
	}

	closeAck := make(chan error)
	closeChannel <- closeAck
	<-closeAck
}
