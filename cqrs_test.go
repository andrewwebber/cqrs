package cqrs_test

import (
	"github.com/andrewwebber/cqrs"
	"log"
	"testing"
	"time"
)

func TestScenario(t *testing.T) {
	persistance := cqrs.NewInMemoryEventStreamRepository()
	bus := cqrs.NewInMemoryEventBus()
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
	log.Println("Dump models")
	log.Println(account)
	log.Println(readModel)
	log.Println(usersModel)

	log.Println("Load the account from history")
	account, error := NewAccountFromHistory(accountID, repository)
	if error != nil {
		t.Fatal(error)
	}

	log.Println("Dump models")
	log.Println(account)
	log.Println(readModel)
	log.Println(usersModel)

	log.Println("Change the email address, credit 150, debit 200")
	lastEmailAddress := "john.snow@golang.org"
	account.ChangeEmailAddress(lastEmailAddress)
	account.Credit(150)
	account.Debit(200)
	log.Println(account)
	log.Println(readModel)

	log.Println("Persist the account")
	repository.Save(account)
	log.Println("Dump models")
	log.Println(account)
	log.Println(readModel)
	log.Println(usersModel)

	log.Println("Load the account from history")
	account, error = NewAccountFromHistory(accountID, repository)
	if error != nil {
		t.Fatal(error)
	}

	time.Sleep(100 * time.Millisecond)
	// All events should have been replayed and the email address should be the latest
	log.Println("Dump models")
	log.Println(account)
	log.Println(readModel)
	log.Println(usersModel)
	// log.Println(usersModel)
	if account.EmailAddress != lastEmailAddress {
		t.Fatal("Expected emailaddress to be ", lastEmailAddress)
	}

	if account.Balance != readModel.Accounts[accountID].Balance {
		t.Fatal("Expected readmodel to be synced with write model")
	}

	stopChannel <- true
}
