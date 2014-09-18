package example_test

import (
	"encoding/json"
	"fmt"
	"github.com/andrewwebber/cqrs"
	"io/ioutil"
	"log"
	"os"
)

type AccountReadModel struct {
	ID           string
	FirstName    string
	LastName     string
	EmailAddress string
	Balance      float64
}

func (account *AccountReadModel) String() string {
	return fmt.Sprintf("ReadModel::Account %s with Email Address %s has balance %f", account.ID, account.EmailAddress, account.Balance)
}

type ReadModelPublisher struct {
	Accounts map[string]*AccountReadModel
}

func (model *ReadModelPublisher) String() string {
	result := "ReadModel::"
	for key := range model.Accounts {
		result += model.Accounts[key].String() + "\n"
	}

	return result
}

func (model *ReadModelPublisher) LoadAccounts(persistance cqrs.EventStreamRepository, repository cqrs.TypeRegistry) {
	readBytes, error := ioutil.ReadFile("/tmp/accounts.json")

	if !os.IsNotExist(error) {
		fmt.Println("Loading accounts from disk")
		json.Unmarshal(readBytes, &model.Accounts)
	} else {
		fmt.Println("Replaying events from repository")
		events, error := persistance.Get("5058e029-d329-4c4b-b111-b042e48b0c5f", repository)
		if error == nil {
			model.PublishEvents(events)
		}
	}
}

func NewReadModelPublisher() *ReadModelPublisher {
	return &ReadModelPublisher{make(map[string]*AccountReadModel)}
}

func NewReadModelPublisherFromHistory(events []cqrs.VersionedEvent) (*ReadModelPublisher, error) {
	publisher := NewReadModelPublisher()
	if error := publisher.PublishEvents(events); error != nil {
		return nil, error
	}

	return publisher, nil
}

func (model *ReadModelPublisher) PublishEvents(events []cqrs.VersionedEvent) error {

	for _, event := range events {
		log.Println("ViewModel received event : ", event)
		switch event.Event.(type) {
		default:
		case *AccountCreatedEvent:
			model.UpdateViewModelOnAccountCreatedEvent(event.SourceID, event.Event.(*AccountCreatedEvent))
		case *AccountCreditedEvent:
			model.UpdateViewModelOnAccountCreditedEvent(event.SourceID, event.Event.(*AccountCreditedEvent))
		case *AccountDebitedEvent:
			model.UpdateViewModelOnAccountDebitedEvent(event.SourceID, event.Event.(*AccountDebitedEvent))
		case *EmailAddressChangedEvent:
			model.UpdateViewModelOnEmailAddressChangedEvent(event.SourceID, event.Event.(*EmailAddressChangedEvent))
		}
	}

	bytes, error := json.Marshal(model.Accounts)
	if error != nil {
		return error
	}

	error = ioutil.WriteFile("/tmp/accounts.json", bytes, 0644)
	if error != nil {
		return error
	}

	return nil
}

func (model *ReadModelPublisher) UpdateViewModelOnAccountCreatedEvent(accountID string, event *AccountCreatedEvent) {
	model.Accounts[accountID] = &AccountReadModel{accountID, event.FirstName, event.LastName, event.EmailAddress, event.InitialBalance}
}

func (model *ReadModelPublisher) UpdateViewModelOnAccountCreditedEvent(accountID string, event *AccountCreditedEvent) {
	model.Accounts[accountID].Balance += event.Amount
}

func (model *ReadModelPublisher) UpdateViewModelOnAccountDebitedEvent(accountID string, event *AccountDebitedEvent) {
	model.Accounts[accountID].Balance -= event.Amount
}

func (model *ReadModelPublisher) UpdateViewModelOnEmailAddressChangedEvent(accountID string, event *EmailAddressChangedEvent) {
	model.Accounts[accountID].EmailAddress = event.NewEmailAddress
}
