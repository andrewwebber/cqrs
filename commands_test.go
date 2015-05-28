package cqrs_test

import (
	"log"
	"testing"

	"github.com/andrewwebber/cqrs"
)

type SampleMessageCommand struct {
	Message string
}

func TestCommandDispatcher(t *testing.T) {
	dispatcher := cqrs.NewMapBasedCommandDispatcher()
	success := false
	dispatcher.RegisterCommandHandler(SampleMessageCommand{}, func(command cqrs.Command) error {
		log.Println("Received Command : ", command.Body.(SampleMessageCommand).Message)
		success = true
		return nil
	})

	dispatcher.DispatchCommand(cqrs.Command{Body: SampleMessageCommand{"Hello world"}})
	if !success {
		t.Fatal("Expected success")
	}
}
