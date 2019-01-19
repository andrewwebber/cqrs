package cqrs_test

import (
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
		cqrs.PackageLogger().Debugf("Received Command : ", command.Body.(SampleMessageCommand).Message)
		success = true
		return nil
	})

	err := dispatcher.DispatchCommand(cqrs.Command{Body: SampleMessageCommand{"Hello world"}})
	if err != nil {
		t.Fatal(err)
	}
	if !success {
		t.Fatal("Expected success")
	}
}
