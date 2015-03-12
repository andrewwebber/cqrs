package cqrs_test

import (
	"log"
	"testing"
	"time"

	"github.com/andrewwebber/cqrs"
	"github.com/logrusorgru/glr"
)

var accountID = "5058e029-d329-4c4b-b111-b042e48b0c5f"

func TestScenario(t *testing.T) {
	log.SetFlags(log.LstdFlags | log.Lshortfile)
	// Type Registry
	typeRegistry := cqrs.NewTypeRegistry()

	// Event sourcing
	persistance := cqrs.NewInMemoryEventStreamRepository()
	bus := cqrs.NewInMemoryEventBus()
	repository := cqrs.NewRepositoryWithPublisher(persistance, bus, typeRegistry)
	typeRegistry.RegisterAggregate(&Account{}, AccountCreatedEvent{}, EmailAddressChangedEvent{}, AccountCreditedEvent{}, AccountDebitedEvent{}, PasswordChangedEvent{})

	// Read Models
	readModel := NewReadModelAccounts()
	usersModel := NewUsersModel()

	// Command Handlers
	commandBus := cqrs.NewInMemoryCommandBus()
	commandDispatcher := cqrs.NewCommandDispatchManager(commandBus, typeRegistry)
	RegisterCommandHandlers(commandDispatcher, repository)

	// Integration events
	eventDispatcher := cqrs.NewVersionedEventDispatchManager(bus, typeRegistry)
	integrationEventsLog := cqrs.NewInMemoryEventStreamRepository()
	RegisterIntegrationEventHandlers(eventDispatcher, integrationEventsLog, readModel, usersModel)

	commandDispatcherStopChannel := make(chan bool)
	eventDispatcherStopChannel := make(chan bool)
	go commandDispatcher.Listen(commandDispatcherStopChannel)
	go eventDispatcher.Listen(eventDispatcherStopChannel)

	log.Println("Dump models")
	log.Println(readModel)
	log.Println(usersModel)

	log.Println("Find an account")
	readModelAccount := readModel.Accounts[accountID]
	log.Println(readModelAccount)

	log.Println("Find a user")
	user := usersModel.Users[accountID]
	log.Println(user)

	hashedPassword, err := GetHashForPassword("$ThisIsMyPassword1")
	if err != nil {
		t.Fatal("Error: ", err)
	}

	log.Println("Create new account...")
	createAccountCommand := cqrs.CreateCommand(
		CreateAccountCommand{"John", "Snow", "John.Snow@thewall.eu", hashedPassword, 0.0})
	commandBus.PublishCommands([]cqrs.Command{createAccountCommand})

	log.Println("Dump models")
	log.Println(readModel)
	log.Println(usersModel)

	log.Println("Change Password")
	changePasswordCommand := cqrs.CreateCommand(
		ChangePasswordCommand{accountID, "$ThisIsANOTHERPassword"})
	commandBus.PublishCommands([]cqrs.Command{changePasswordCommand})

	log.Println("Change email address and credit the account")
	changeEmailAddressCommand := cqrs.CreateCommand(
		ChangeEmailAddressCommand{accountID, "john.snow@the.wall"})
	creditAccountCommand := cqrs.CreateCommand(
		CreditAccountCommand{accountID, 50})
	creditAccountCommand2 := cqrs.CreateCommand(
		CreditAccountCommand{accountID, 50})
	commandBus.PublishCommands([]cqrs.Command{
		changeEmailAddressCommand,
		creditAccountCommand,
		creditAccountCommand2})

	log.Println("Dump models")
	log.Println(readModel)
	log.Println(usersModel)

	log.Println("Change the email address, credit 150, debit 200")
	lastEmailAddress := "john.snow@golang.org"
	changeEmailAddressCommand = cqrs.CreateCommand(
		ChangeEmailAddressCommand{accountID, lastEmailAddress})
	creditAccountCommand = cqrs.CreateCommand(
		CreditAccountCommand{accountID, 150})
	debitAccountCommand := cqrs.CreateCommand(
		DebitAccountCommand{accountID, 200})
	commandBus.PublishCommands([]cqrs.Command{
		changeEmailAddressCommand,
		creditAccountCommand,
		debitAccountCommand})

	log.Println("Dump models")
	log.Println(readModel)
	log.Println(usersModel)

	time.Sleep(300 * time.Millisecond)
	log.Println("Dump history - integration events")
	if history, err := repository.GetEventStreamRepository().AllIntegrationEventsEverPublished(); err != nil {
		t.Fatal(err)
	} else {
		for _, event := range history {
			log.Println(event)
		}
	}

	log.Println("GetIntegrationEventsByCorrelationID")
	correlationEvents, err := repository.GetEventStreamRepository().GetIntegrationEventsByCorrelationID(debitAccountCommand.CorrelationID)
	if err != nil || len(correlationEvents) == 0 {
		t.Fatal(err)
	}

	for correlationEvent := range correlationEvents {
		log.Println(correlationEvent)
	}

	log.Println("Load the account from history")
	account, error := NewAccountFromHistory(accountID, repository)
	if error != nil {
		t.Fatal(error)
	}

	// All events should have been replayed and the email address should be the latest
	log.Println("Dump models")
	log.Println(account)
	log.Println(readModel)
	log.Println(usersModel)

	if account.EmailAddress != lastEmailAddress {
		t.Fatal("Expected emailaddress to be ", lastEmailAddress)
	}

	if account.Balance != readModel.Accounts[accountID].Balance {
		t.Fatal("Expected readmodel to be synced with write model")
	}

	eventDispatcherStopChannel <- true
	commandDispatcherStopChannel <- true
}

func RegisterIntegrationEventHandlers(eventDispatcher *cqrs.VersionedEventDispatchManager, integrationEventsLog cqrs.VersionedEventPublicationLogger, readModel *ReadModelAccounts, usersModel *UsersModel) {
	eventDispatcher.RegisterGlobalHandler(func(event cqrs.VersionedEvent) error {
		integrationEventsLog.SaveIntegrationEvent(event)
		readModel.UpdateViewModel([]cqrs.VersionedEvent{event})
		usersModel.UpdateViewModel([]cqrs.VersionedEvent{event})
		return nil
	})
}

func RegisterCommandHandlers(commandDispatcher *cqrs.CommandDispatchManager, repository cqrs.EventSourcingRepository) {
	commandDispatcher.RegisterCommandHandler(CreateAccountCommand{}, func(command cqrs.Command) error {
		createAccountCommand := command.Body.(CreateAccountCommand)
		log.Println(glr.Green("Processing command - Create account"))
		account := NewAccount(createAccountCommand.FirstName,
			createAccountCommand.LastName,
			createAccountCommand.EmailAddress,
			createAccountCommand.PasswordHash,
			createAccountCommand.InitialBalance)

		log.Println(glr.Green("Set ID..."))
		account.SetID(accountID)
		log.Println(account)
		log.Println(glr.Green("Persist the account"))
		repository.Save(account, command.CorrelationID)
		log.Println(glr.Green(account.String()))
		return nil
	})

	commandDispatcher.RegisterCommandHandler(ChangeEmailAddressCommand{}, func(command cqrs.Command) error {
		changeEmailAddressCommand := command.Body.(ChangeEmailAddressCommand)
		log.Println(glr.Green("Processing command - Change email address"))
		account, err := NewAccountFromHistory(changeEmailAddressCommand.AccountID, repository)
		if err != nil {
			return err
		}

		account.ChangeEmailAddress(changeEmailAddressCommand.NewEmailAddress)
		log.Println(glr.Green("Persist the account"))
		repository.Save(account, command.CorrelationID)
		log.Println(glr.Green(account.String()))
		return nil
	})

	commandDispatcher.RegisterCommandHandler(ChangePasswordCommand{}, func(command cqrs.Command) error {
		changePasswordCommand := command.Body.(ChangePasswordCommand)
		log.Println(glr.Green("Processing command - Change password"))
		account, err := NewAccountFromHistory(changePasswordCommand.AccountID, repository)
		if err != nil {
			return err
		}

		account.ChangePassword(changePasswordCommand.NewPassword)
		log.Println(glr.Green("Persist the account"))
		repository.Save(account, command.CorrelationID)
		log.Println(glr.Green(account.String()))
		return nil
	})

	commandDispatcher.RegisterCommandHandler(CreditAccountCommand{}, func(command cqrs.Command) error {
		creditAccountCommand := command.Body.(CreditAccountCommand)
		log.Println(glr.Green("Processing command - Credit account"))
		account, err := NewAccountFromHistory(creditAccountCommand.AccountID, repository)
		if err != nil {
			return err
		}

		account.Credit(creditAccountCommand.Amount)
		log.Println(glr.Green("Persist the account"))
		repository.Save(account, command.CorrelationID)
		log.Println(glr.Green(account.String()))
		return nil
	})

	commandDispatcher.RegisterCommandHandler(DebitAccountCommand{}, func(command cqrs.Command) error {
		debitAccountCommand := command.Body.(DebitAccountCommand)
		log.Println(glr.Green("Processing command - Debit account"))
		account, err := NewAccountFromHistory(debitAccountCommand.AccountID, repository)
		if err != nil {
			return err
		}

		if err := account.Debit(debitAccountCommand.Amount); err != nil {
			return err
		}

		log.Println(glr.Green("Persist the account"))
		repository.Save(account, command.CorrelationID)
		log.Println(glr.Green(account.String()))
		return nil
	})
}
