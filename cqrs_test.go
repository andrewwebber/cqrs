package cqrs_test

import (
	"errors"
	"github.com/andrewwebber/cqrs"
	"log"
	"testing"
)

type AccountCreatedEvent struct {
	FirstName    string
	LastName     string
	EmailAddress string
}

type EmailAddressChangedEvent struct {
	PreviousEmailAddress string
	NewEmailAddress      string
}

type Account struct {
	cqrs.EventSourceBased

	FirstName    string
	LastName     string
	EmailAddress string
}

func NewAccount(firstName string, lastName string, emailAddress string) *Account {
	account := new(Account)
	account.EventSourceBased = cqrs.NewEventSourceBased(account)

	event := AccountCreatedEvent{firstName, lastName, emailAddress}
	account.Update(&event)
	return account
}

func (account *Account) ChangeEmailAddress(newEmailAddress string) error {
	if len(newEmailAddress) < 1 {
		return errors.New("Invalid newEmailAddress length")
	}

	account.Update(&EmailAddressChangedEvent{account.EmailAddress, newEmailAddress})
	return nil
}

func (account *Account) HandleAccountCreatedEvent(event *AccountCreatedEvent) {
	account.EmailAddress = event.EmailAddress
	account.FirstName = event.FirstName
	account.LastName = event.LastName
	log.Println("HandleAccountCreatedEvent")
}

func (account *Account) HandleUsernameChangedEvent(event *EmailAddressChangedEvent) {
	account.EmailAddress = event.NewEmailAddress
	log.Println("HandleEmailAddressChangedEvent")
}

func TestEventSourcing(t *testing.T) {
	cqrs.RegisterType(&AccountCreatedEvent{})
	cqrs.RegisterType(&EmailAddressChangedEvent{})
	cqrs.RegisterType(&Account{})

	account := NewAccount("John", "Snow", "john.snow@cqrs.example")
	account.SetID("5058e029-d329-4c4b-b111-b042e48b0c5f")
	account.ChangeEmailAddress("john.snow@the.wall")
	for _, event := range account.Events() {
		log.Println("Event ", event)
	}

	log.Println(account.EmailAddress)

	repository := cqrs.NewRepository()
	repository.Save(account)

	var accountFromEvents Account
	error := repository.Get("5058e029-d329-4c4b-b111-b042e48b0c5f", &accountFromEvents)
	if error != nil {
		log.Println(error)
	}

	log.Println(accountFromEvents.EmailAddress)
}
