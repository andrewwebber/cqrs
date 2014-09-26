package cqrs_test

import (
	"fmt"
	"github.com/andrewwebber/cqrs"
	"github.com/logrusorgru/golorize"
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
		log.Println(golorize.Magenta("Accounts Model received event : " + event.EventType))
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

	return nil
}

func (model *ReadModelAccounts) UpdateViewModelOnAccountCreatedEvent(accountID string, event AccountCreatedEvent) {
	model.Accounts[accountID] = &AccountReadModel{accountID, event.FirstName, event.LastName, event.EmailAddress, event.InitialBalance}
}

func (model *ReadModelAccounts) UpdateViewModelOnAccountCreditedEvent(accountID string, event AccountCreditedEvent) {
	if model.Accounts[accountID] == nil {
		log.Println(golorize.Red("Could not find account with ID " + accountID))
		return
	}

	model.Accounts[accountID].Balance += event.Amount
}

func (model *ReadModelAccounts) UpdateViewModelOnAccountDebitedEvent(accountID string, event AccountDebitedEvent) {
	if model.Accounts[accountID] == nil {
		log.Println(golorize.Red("Could not find account with ID " + accountID))
		return
	}

	model.Accounts[accountID].Balance -= event.Amount
}

func (model *ReadModelAccounts) UpdateViewModelOnEmailAddressChangedEvent(accountID string, event EmailAddressChangedEvent) {
	if model.Accounts[accountID] == nil {
		log.Println(golorize.Red("Could not find account with ID " + accountID))
		return
	}

	model.Accounts[accountID].EmailAddress = event.NewEmailAddress
}

type User struct {
	ID           string
	FirstName    string
	LastName     string
	EmailAddress string
	PasswordHash []byte
}

func (user *User) String() string {
	return fmt.Sprintf("UserModel::User %s with Email Address %s and Password Hash %v", user.ID, user.EmailAddress, user.PasswordHash)
}

type UsersModel struct {
	Users map[string]*User
}

func (model *UsersModel) String() string {
	result := "User Model::"
	for key := range model.Users {
		result += model.Users[key].String() + "\n"
	}

	return result
}

func NewUsersModel() *UsersModel {
	return &UsersModel{make(map[string]*User)}
}

func NewUsersModelFromHistory(events []cqrs.VersionedEvent) (*UsersModel, error) {
	publisher := NewUsersModel()
	if error := publisher.UpdateViewModel(events); error != nil {
		return nil, error
	}

	return publisher, nil
}

func (model *UsersModel) UpdateViewModel(events []cqrs.VersionedEvent) error {
	for _, event := range events {
		log.Println("User Model received event : ", event.EventType)
		switch event.Event.(type) {
		default:
		case AccountCreatedEvent:
			model.UpdateViewModelOnAccountCreatedEvent(event.SourceID, event.Event.(AccountCreatedEvent))
		case EmailAddressChangedEvent:
			model.UpdateViewModelOnEmailAddressChangedEvent(event.SourceID, event.Event.(EmailAddressChangedEvent))
		case PasswordChangedEvent:
			model.UpdateViewModelOnPasswordChangedEvent(event.SourceID, event.Event.(PasswordChangedEvent))
		}
	}

	return nil
}

func (model *UsersModel) UpdateViewModelOnAccountCreatedEvent(accountID string, event AccountCreatedEvent) {
	model.Users[accountID] = &User{accountID, event.FirstName, event.LastName, event.EmailAddress, event.PasswordHash}
}

func (model *UsersModel) UpdateViewModelOnEmailAddressChangedEvent(accountID string, event EmailAddressChangedEvent) {

	if model.Users[accountID] == nil {
		return
	}

	model.Users[accountID].EmailAddress = event.NewEmailAddress
}

func (model *UsersModel) UpdateViewModelOnPasswordChangedEvent(accountID string, event PasswordChangedEvent) {

	if model.Users[accountID] == nil {
		return
	}

	model.Users[accountID].PasswordHash = event.NewPasswordHash
}
