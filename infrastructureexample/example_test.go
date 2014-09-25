package infastructureexample_test

import (
	"code.google.com/p/go.crypto/bcrypt"
	"errors"
	"fmt"
	"github.com/andrewwebber/cqrs"
	"github.com/andrewwebber/cqrs/couchbase"
	"github.com/andrewwebber/cqrs/rabbit"
	// "github.com/andrewwebber/cqrs/rethinkdb"
	// r "github.com/dancannon/gorethink"
	"log"
	"testing"
)

type AccountCreatedEvent struct {
	FirstName      string
	LastName       string
	EmailAddress   string
	PasswordHash   []byte
	InitialBalance float64
}

type EmailAddressChangedEvent struct {
	PreviousEmailAddress string
	NewEmailAddress      string
}

type PasswordChangedEvent struct {
	NewPasswordHash []byte
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
	PasswordHash []byte
	Balance      float64
}

func (account *Account) String() string {
	return fmt.Sprintf("Account %s with Email Address %s has balance %f", account.ID(), account.EmailAddress, account.Balance)
}

func NewAccount(firstName string, lastName string, emailAddress string, passwordHash []byte, initialBalance float64) *Account {
	account := new(Account)
	account.EventSourceBased = cqrs.NewEventSourceBased(account)

	event := AccountCreatedEvent{firstName, lastName, emailAddress, passwordHash, initialBalance}
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
	account.PasswordHash = event.PasswordHash
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

func (account *Account) CheckPassword(password string) bool {

	passwordBytes := []byte(password)

	// Comparing the password with the hash
	err := bcrypt.CompareHashAndPassword(account.PasswordHash, passwordBytes)
	fmt.Println(err) // nil means it is a match

	return err == nil
}

func (account *Account) ChangePassword(newPassword string) error {
	if len(newPassword) < 1 {
		return errors.New("Invalid newPassword length")
	}

	hashedPassword, err := GetHashForPassword(newPassword)
	if err != nil {
		panic(err)
	}

	account.Update(PasswordChangedEvent{hashedPassword})

	return nil
}

func GetHashForPassword(password string) ([]byte, error) {
	fmt.Println("Password: ", password)

	passwordBytes := []byte(password)

	fmt.Println("Password bytes: ", passwordBytes)

	// Hashing the password with the cost of 10
	hashedPassword, err := bcrypt.GenerateFromPassword(passwordBytes, 10)
	if err != nil {
		fmt.Println("Error getting password hash: ", err)
		return nil, err
	}

	fmt.Println(string(hashedPassword))

	return hashedPassword, nil
}

func (account *Account) HandlePasswordChangedEvent(event PasswordChangedEvent) {
	account.PasswordHash = event.NewPasswordHash
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

// func TestEventSourcingWithRethinkdb(t *testing.T) {
// 	connectOps := r.ConnectOpts{Address: "localhost:28015", Database: "cqrs"}
// 	session, error := r.Connect(connectOps)
// 	r.Table("events").Delete().Run(session)
//
// 	persistance, error := rethinkdb.NewEventStreamRepository(connectOps, "events")
// 	if error != nil {
// 		t.Fatal(error)
// 	}
//
// 	r.Table("events").Delete().Run(session)
//
// 	RunScenario(t, persistance)
// }

func TestEventSourcingWithCouchbase(t *testing.T) {
	persistance, error := couchbase.NewEventStreamRepository("http://localhost:8091/")
	if error != nil {
		t.Fatal(error)
	}

	RunScenario(t, persistance)
}

func RunScenario(t *testing.T, persistance cqrs.EventStreamRepository) {
	bus := rabbit.NewEventBus("amqp://guest:guest@localhost:5672/", "example_test", "testing.example")
	repository := cqrs.NewRepositoryWithPublisher(persistance, bus)
	repository.RegisterAggregate(&Account{}, AccountCreatedEvent{}, EmailAddressChangedEvent{}, AccountCreditedEvent{}, AccountDebitedEvent{}, PasswordChangedEvent{})
	accountID := "5058e029-d329-4c4b-b111-b042e48b0c5f"

	readModel := NewReadModelAccounts()

	usersModel := NewUsersModel()

	eventDispatcher := cqrs.NewVersionedEventDispatchManager(bus)
	eventDispatcher.RegisterEventHandler(AccountCreatedEvent{}, func(event cqrs.VersionedEvent) error {
		readModel.UpdateViewModel([]cqrs.VersionedEvent{event})
		usersModel.UpdateViewModel([]cqrs.VersionedEvent{event})
		return nil
	})

	eventDispatcher.RegisterEventHandler(AccountCreditedEvent{}, func(event cqrs.VersionedEvent) error {
		readModel.UpdateViewModel([]cqrs.VersionedEvent{event})
		return nil
	})

	eventDispatcher.RegisterEventHandler(AccountDebitedEvent{}, func(event cqrs.VersionedEvent) error {
		readModel.UpdateViewModel([]cqrs.VersionedEvent{event})
		return nil
	})

	eventDispatcher.RegisterEventHandler(EmailAddressChangedEvent{}, func(event cqrs.VersionedEvent) error {
		readModel.UpdateViewModel([]cqrs.VersionedEvent{event})
		usersModel.UpdateViewModel([]cqrs.VersionedEvent{event})
		return nil
	})

	eventDispatcher.RegisterEventHandler(PasswordChangedEvent{}, func(event cqrs.VersionedEvent) error {
		usersModel.UpdateViewModel([]cqrs.VersionedEvent{event})
		return nil
	})

	stopChannel := make(chan bool)
	go eventDispatcher.Listen(stopChannel)

	readModel.LoadAccounts(persistance, repository)

	usersModel.LoadUsers(persistance, repository)

	log.Println("Loaded accounts")
	log.Println(readModel)

	log.Println("Loaded Users")
	log.Println(usersModel)

	log.Println("Create or find an account")
	readModelAccount := readModel.Accounts[accountID]

	log.Println(readModelAccount)

	log.Println("Create or find a user")
	user := usersModel.Users[accountID]

	log.Println(user)

	var account *Account

	if readModelAccount == nil {
		log.Println("Get hash for user...")

		hashedPassword, err := GetHashForPassword("$ThisIsMyPassword1")

		if err != nil {
			t.Fatal("Error: ", err)
		}

		log.Println("Get hash for user...")

		log.Println("Create new account...")
		account = NewAccount("John", "Snow", "john.snow@cqrs.example", hashedPassword, 0.0)

		log.Println("Set ID...")
		account.SetID(accountID)

		log.Println(account)
	} else {
		account, _ = NewAccountFromHistory(accountID, repository)
	}

	log.Println(account)
	log.Println(readModel)
	log.Println(usersModel)

	account.ChangePassword("$ThisIsANOTHERPassword")

	if !account.CheckPassword("$ThisIsANOTHERPassword") {
		t.Fatal("Password is incorrect for account")
	}

	log.Println("Change email address and credit the account")
	account.ChangeEmailAddress("john.snow@the.wall")
	account.Credit(50)
	account.Credit(50)
	log.Println(account)
	log.Println(readModel)

	log.Println("Persist the account")
	repository.Save(account)
	log.Println(readModel)
	log.Println(usersModel)

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
	log.Println(usersModel)

	log.Println("Load the account from history")
	account, error = NewAccountFromHistory(accountID, repository)
	if error != nil {
		t.Fatal(error)
	}

	// All events should have been replayed and the email address should be the latest
	log.Println(account)
	log.Println(readModel)
	// log.Println(usersModel)
	if account.EmailAddress != lastEmailAddress {
		t.Fatal("Expected emailaddress to be ", lastEmailAddress)
	}

	stopChannel <- true
}
