package example_test

import (
	"errors"
	"fmt"
	"github.com/andrewwebber/cqrs"
	"github.com/andrewwebber/cqrs/couchbase"
	"github.com/andrewwebber/cqrs/rethinkdb"
	r "github.com/dancannon/gorethink"
	"log"
	"testing"
)

type AccountCreatedEvent struct {
	FirstName      string
	LastName       string
	EmailAddress   string
	InitialBalance float64
}

type EmailAddressChangedEvent struct {
	PreviousEmailAddress string
	NewEmailAddress      string
}

type AccountCreditedEvent struct {
	Amount float64
}

type AccountDebitedEvent struct {
	Amount float64
}

type Account struct {
	cqrs.EventSourceBased

	FirstName    string
	LastName     string
	EmailAddress string
	Balance      float64
}

func (account *Account) String() string {
	return fmt.Sprintf("Account %s with Email Address %s has balance %f", account.ID(), account.EmailAddress, account.Balance)
}

func NewAccount(firstName string, lastName string, emailAddress string, initialBalance float64) *Account {
	account := new(Account)
	account.EventSourceBased = cqrs.NewEventSourceBased(account)

	event := AccountCreatedEvent{firstName, lastName, emailAddress, initialBalance}
	account.Update(event)
	return account
}

func NewAccountFromHistory(id string, repository cqrs.EventSourcingRepository) (*Account, error) {
	account := new(Account)
	account.EventSourceBased = cqrs.NewEventSourceBasedWithID(account, id)

	if error := repository.Get(id, account); error != nil {
		return account, error
	}

	return account, nil
}

func (account *Account) HandleAccountCreatedEvent(event AccountCreatedEvent) {
	account.EmailAddress = event.EmailAddress
	account.FirstName = event.FirstName
	account.LastName = event.LastName
}

func (account *Account) ChangeEmailAddress(newEmailAddress string) error {
	if len(newEmailAddress) < 1 {
		return errors.New("Invalid newEmailAddress length")
	}

	account.Update(EmailAddressChangedEvent{account.EmailAddress, newEmailAddress})
	return nil
}

func (account *Account) HandleEmailAddressChangedEvent(event EmailAddressChangedEvent) {
	account.EmailAddress = event.NewEmailAddress
}

func (account *Account) Credit(amount float64) error {
	if amount <= 0 {
		return errors.New("Invalid amount - negative credits not supported")
	}

	account.Update(AccountCreditedEvent{amount})

	return nil
}

func (account *Account) HandleAccountCredited(event AccountCreditedEvent) {
	account.Balance += event.Amount
}

func (account *Account) Debit(amount float64) error {
	if amount <= 0 {
		return errors.New("Invalid amount - negative credits not supported")
	}

	if projection := account.Balance - amount; projection < 0 {
		return errors.New("Negative balance not supported")
	}

	account.Update(AccountDebitedEvent{amount})

	return nil
}

func (account *Account) HandleAccountDebitedEvent(event AccountDebitedEvent) {
	account.Balance -= event.Amount
}

func TestEventSourcingWithRethinkdb(t *testing.T) {
	connectOps := r.ConnectOpts{Address: "localhost:28015", Database: "cqrs"}
	session, error := r.Connect(connectOps)
	r.Table("events").Delete().Run(session)

	persistance, error := rethinkdb.NewEventStreamRepository(connectOps, "events")
	if error != nil {
		t.Fatal(error)
	}

	r.Table("events").Delete().Run(session)

	RunScenario(t, persistance)
}

func TestEventSourcingWithCouchbase(t *testing.T) {
	persistance, error := couchbase.NewEventStreamRepository("http://localhost:8091/")
	if error != nil {
		t.Fatal(error)
	}

	RunScenario(t, persistance)
}

func RunScenario(t *testing.T, persistance cqrs.EventStreamRepository) {
	readModel := NewReadModelPublisher()

	repository := cqrs.NewRepositoryWithPublisher(persistance, readModel)
	repository.RegisterAggregate(&Account{}, AccountCreatedEvent{}, EmailAddressChangedEvent{}, AccountCreditedEvent{}, AccountDebitedEvent{})
	accountID := "5058e029-d329-4c4b-b111-b042e48b0c5f"

	readModel.LoadAccounts(persistance, repository)

	log.Println("Loaded accounts")
	log.Println(readModel)

	log.Println("Create or find an account")
	readModelAccount := readModel.Accounts[accountID]

	var account *Account
	if readModelAccount == nil {
		account = NewAccount("John", "Snow", "john.snow@cqrs.example", 0.0)
		account.SetID(accountID)
	} else {
		account, _ = NewAccountFromHistory(accountID, repository)
	}

	log.Println(account)
	log.Println(readModel)

	log.Println("Change email address and credit the account")
	account.ChangeEmailAddress("john.snow@the.wall")
	account.Credit(50)
	account.Credit(50)
	log.Println(account)
	log.Println(readModel)

	log.Println("Persist the account")
	repository.Save(account)
	log.Println(readModel)

	log.Println("Load the account from history")
	account, error := NewAccountFromHistory(accountID, repository)
	if error != nil {
		t.Fatal(error)
	}

	log.Println(account)
	log.Println(readModel)

	log.Println("Change the email address, credit 150, debit 200")
	lastEmailAddress := "john.snow@golang.org"
	account.ChangeEmailAddress(lastEmailAddress)
	account.Credit(150)
	account.Debit(200)
	log.Println(account)
	log.Println(readModel)

	log.Println("Persist the account")
	repository.Save(account)
	log.Println(readModel)

	log.Println("Load the account from history")
	account, error = NewAccountFromHistory(accountID, repository)
	if error != nil {
		t.Fatal(error)
	}

	// All events should have been replayed and the email address should be the latest
	log.Println(account)
	log.Println(readModel)
	if account.EmailAddress != lastEmailAddress {
		t.Fatal("Expected emailaddress to be ", lastEmailAddress)
	}
}
