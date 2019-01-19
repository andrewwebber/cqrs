package cqrs_test

import (
	"errors"
	"reflect"
	"testing"
	"time"

	"github.com/andrewwebber/cqrs"
)

type SampleCommand struct {
	Message string
}

func TestInMemoryCommandBus(t *testing.T) {
	bus := cqrs.NewInMemoryCommandBus()
	CommandType := reflect.TypeOf(SampleCommand{})

	// Create communication channels
	//
	// for closing the queue listener,
	closeChannel := make(chan chan error)
	// receiving errors from the listener thread (go routine)
	errorChannel := make(chan error)
	// and receiving Commands from the queue
	receiveCommandChannel := make(chan cqrs.CommandTransactedAccept)
	commandHandler := func(command cqrs.Command) error {
		accepted := make(chan bool)
		receiveCommandChannel <- cqrs.CommandTransactedAccept{Command: command, ProcessedSuccessfully: accepted}
		if <-accepted {
			return nil
		}

		return errors.New("Unsuccessful")
	}

	// Start receiving Commands by passing these channels to the worker thread (go routine)
	if err := bus.ReceiveCommands(cqrs.CommandReceiverOptions{TypeRegistry: nil, Close: closeChannel, Error: errorChannel, ReceiveCommand: commandHandler}); err != nil {
		t.Fatal(err)
	}

	// Publish a simple Command
	cqrs.PackageLogger().Debugf("Publishing Commands")
	go func() {
		if err := bus.PublishCommands([]cqrs.Command{{
			CommandType: CommandType.String(),
			Created:     time.Now().UTC(),
			Body:        SampleCommand{"TestInMemoryCommandBus"}}}); err != nil {
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
		// Version Command received channel receives a result with a channel to respond to, signifying successful processing of the message.
		// This should Commandually call an Command handler. See cqrs.NewVersionedCommandDispatcher()
	case command := <-receiveCommandChannel:
		sampleCommand := command.Command.Body.(SampleCommand)
		cqrs.PackageLogger().Debugf(sampleCommand.Message)
		command.ProcessedSuccessfully <- true
		// Receiving on this channel signifys an error has occured work processor side
	case err := <-errorChannel:
		t.Fatal(err)
	}

	closeResponse := make(chan error)
	closeChannel <- closeResponse
	<-closeResponse
}
