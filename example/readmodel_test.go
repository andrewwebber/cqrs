package example

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
	return fmt.Sprintf("AccountModel::Account %s with Email Address %s has balance %f", account.ID, account.EmailAddress, account.Balance)
}

type ReadModelAccounts struct {
	Accounts map[string]*AccountReadModel
}

func (model *ReadModelAccounts) String() string {
	result := "Account Model::"
	for key := range model.Accounts {
		result += model.Accounts[key].String() + "\n"
	}

	return result
}

func (model *ReadModelAccounts) LoadAccounts(persistance cqrs.VersionedEventPublicationLogger) {
	readBytes, error := ioutil.ReadFile("/tmp/accounts.json")

	if !os.IsNotExist(error) {
		log.Println("Loading accounts from disk")
		json.Unmarshal(readBytes, &model.Accounts)
	} else {
		log.Println("Replaying events from repository")
		events, error := persistance.AllEventsEverPublished()
		if error == nil {
			model.UpdateViewModel(events)
		}
	}
}

func NewReadModelAccounts() *ReadModelAccounts {
	return &ReadModelAccounts{make(map[string]*AccountReadModel)}
}

func NewReadModelAccountsFromHistory(events []cqrs.VersionedEvent) (*ReadModelAccounts, error) {
	publisher := NewReadModelAccounts()
	if error := publisher.UpdateViewModel(events); error != nil {
		return nil, error
	}

	return publisher, nil
}

func (model *ReadModelAccounts) UpdateViewModel(events []cqrs.VersionedEvent) error {
	for _, event := range events {
		log.Println("Accounts Model received event : ", event.EventType)
		switch event.Event.(type) {
		default:
		case AccountCreatedEvent:
			model.UpdateViewModelOnAccountCreatedEvent(event.SourceID, event.Event.(AccountCreatedEvent))
		case AccountCreditedEvent:
			model.UpdateViewModelOnAccountCreditedEvent(event.SourceID, event.Event.(AccountCreditedEvent))
		case AccountDebitedEvent:
			model.UpdateViewModelOnAccountDebitedEvent(event.SourceID, event.Event.(AccountDebitedEvent))
		case EmailAddressChangedEvent:
			model.UpdateViewModelOnEmailAddressChangedEvent(event.SourceID, event.Event.(EmailAddressChangedEvent))
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

func (model *ReadModelAccounts) UpdateViewModelOnAccountCreatedEvent(accountID string, event AccountCreatedEvent) {
	model.Accounts[accountID] = &AccountReadModel{accountID, event.FirstName, event.LastName, event.EmailAddress, event.InitialBalance}
}

func (model *ReadModelAccounts) UpdateViewModelOnAccountCreditedEvent(accountID string, event AccountCreditedEvent) {
	if model.Accounts[accountID] == nil {
		return
	}

	model.Accounts[accountID].Balance += event.Amount
}

func (model *ReadModelAccounts) UpdateViewModelOnAccountDebitedEvent(accountID string, event AccountDebitedEvent) {
	if model.Accounts[accountID] == nil {
		return
	}

	model.Accounts[accountID].Balance -= event.Amount
}

func (model *ReadModelAccounts) UpdateViewModelOnEmailAddressChangedEvent(accountID string, event EmailAddressChangedEvent) {
	if model.Accounts[accountID] == nil {
		return
	}

	model.Accounts[accountID].EmailAddress = event.NewEmailAddress
}
