package example_test

import (
	"fmt"
	"github.com/andrewwebber/cqrs"
	"log"
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
