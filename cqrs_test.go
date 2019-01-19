package cqrs_test

import (
	"fmt"
	"log"
	"testing"
	"time"

	"github.com/andrewwebber/cqrs"

	"github.com/stretchr/testify/require"
)

var accountID = cqrs.NewUUIDString()

func TestScenario(t *testing.T) {

	log.SetFlags(log.LstdFlags | log.Lshortfile)

	// Type Registry
	typeRegistry := cqrs.NewTypeRegistry()

	// Event sourcing
	persistance := cqrs.NewInMemoryEventStreamRepository()
	bus := cqrs.NewInMemoryEventBus()
	repository := cqrs.NewRepositoryWithPublisher(persistance, bus, typeRegistry)
	typeRegistry.RegisterAggregate(&Account{})
	typeRegistry.RegisterEvents(AccountCreatedEvent{}, EmailAddressChangedEvent{}, AccountCreditedEvent{}, AccountDebitedEvent{}, PasswordChangedEvent{})

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
	go func() {
		err := commandDispatcher.Listen(commandDispatcherStopChannel, false, 1)
		require.NoError(t, err)
	}()
	go func() {
		err := eventDispatcher.Listen(eventDispatcherStopChannel, false, 1)
		require.NoError(t, err)
	}()

	cqrs.PackageLogger().Debugf("Dump models")
	cqrs.PackageLogger().Debugf(fmt.Sprintf("%+v", readModel))
	cqrs.PackageLogger().Debugf(fmt.Sprintf("%+v", usersModel))

	cqrs.PackageLogger().Debugf("Find an account")
	readModelAccount := readModel.GetAccount(accountID)
	cqrs.PackageLogger().Debugf(fmt.Sprintf("%+v", readModelAccount))

	cqrs.PackageLogger().Debugf("Find a user")
	user := usersModel.Users[accountID]
	cqrs.PackageLogger().Debugf(fmt.Sprintf("%+v", user))

	hashedPassword, err := GetHashForPassword("$ThisIsMyPassword1")
	if err != nil {
		t.Fatal("Error: ", err)
	}

	cqrs.PackageLogger().Debugf("Create new account...")
	createAccountCommand := cqrs.CreateCommand(
		CreateAccountCommand{"John", "Snow", "John.Snow@thewall.eu", hashedPassword, 0.0})
	err = commandBus.PublishCommands([]cqrs.Command{createAccountCommand})
	require.NoError(t, err)

	cqrs.PackageLogger().Debugf("Dump models")
	cqrs.PackageLogger().Debugf(fmt.Sprintf("%+v", readModel))
	cqrs.PackageLogger().Debugf(fmt.Sprintf("%+v", usersModel))

	cqrs.PackageLogger().Debugf("Change Password")
	changePasswordCommand := cqrs.CreateCommand(
		ChangePasswordCommand{accountID, "$ThisIsANOTHERPassword"})
	err = commandBus.PublishCommands([]cqrs.Command{changePasswordCommand})
	require.NoError(t, err)

	cqrs.PackageLogger().Debugf("Change email address and credit the account")
	changeEmailAddressCommand := cqrs.CreateCommand(
		ChangeEmailAddressCommand{accountID, "john.snow@the.wall"})
	creditAccountCommand := cqrs.CreateCommand(
		CreditAccountCommand{accountID, 50})
	creditAccountCommand2 := cqrs.CreateCommand(
		CreditAccountCommand{accountID, 50})
	err = commandBus.PublishCommands([]cqrs.Command{
		changeEmailAddressCommand,
		creditAccountCommand,
		creditAccountCommand2})
	require.NoError(t, err)

	cqrs.PackageLogger().Debugf("Dump models")
	cqrs.PackageLogger().Debugf(fmt.Sprintf("%+v", readModel))
	cqrs.PackageLogger().Debugf(fmt.Sprintf("%+v", usersModel))

	cqrs.PackageLogger().Debugf("Change the email address, credit 150, debit 200")
	lastEmailAddress := "john.snow@golang.org"
	changeEmailAddressCommand = cqrs.CreateCommand(
		ChangeEmailAddressCommand{accountID, lastEmailAddress})
	creditAccountCommand = cqrs.CreateCommand(
		CreditAccountCommand{accountID, 150})
	debitAccountCommand := cqrs.CreateCommand(
		DebitAccountCommand{accountID, 200})
	err = commandBus.PublishCommands([]cqrs.Command{
		changeEmailAddressCommand,
		creditAccountCommand,
		debitAccountCommand})
	require.NoError(t, err)

	cqrs.PackageLogger().Debugf("Dump models")
	cqrs.PackageLogger().Debugf(fmt.Sprintf("%+v", readModel))
	cqrs.PackageLogger().Debugf(fmt.Sprintf("%+v", usersModel))

	time.Sleep(300 * time.Millisecond)
	cqrs.PackageLogger().Debugf("Dump history - integration events")
	if history, err := repository.GetEventStreamRepository().AllIntegrationEventsEverPublished(); err != nil {
		t.Fatal(err)
	} else {
		for _, event := range history {
			cqrs.PackageLogger().Debugf(fmt.Sprintf("%+v", event))
		}
	}

	cqrs.PackageLogger().Debugf("GetIntegrationEventsByCorrelationID")
	correlationEvents, err := repository.GetEventStreamRepository().GetIntegrationEventsByCorrelationID(debitAccountCommand.CorrelationID)
	if err != nil || len(correlationEvents) == 0 {
		t.Fatal(err)
	}

	for _, correlationEvent := range correlationEvents {
		cqrs.PackageLogger().Debugf(fmt.Sprintf("%+v", correlationEvent))
	}

	cqrs.PackageLogger().Debugf("Load the account from history")
	account, error := NewAccountFromHistory(accountID, repository)
	if error != nil {
		t.Fatal(error)
	}

	// All events should have been replayed and the email address should be the latest
	cqrs.PackageLogger().Debugf("Dump models")
	cqrs.PackageLogger().Debugf(fmt.Sprintf("%+v", account))
	cqrs.PackageLogger().Debugf(fmt.Sprintf("%+v", readModel))
	cqrs.PackageLogger().Debugf(fmt.Sprintf("%+v", usersModel))

	if account.EmailAddress != lastEmailAddress {
		t.Fatal("Expected emailaddress to be ", lastEmailAddress)
	}

	if account.Balance != readModel.GetAccount(accountID).Balance {
		t.Fatal("Expected readmodel to be synced with write model")
	}

	eventDispatcherStopChannel <- true
	commandDispatcherStopChannel <- true
}

func RegisterIntegrationEventHandlers(eventDispatcher *cqrs.VersionedEventDispatchManager, integrationEventsLog cqrs.VersionedEventPublicationLogger, readModel *ReadModelAccounts, usersModel *UsersModel) {
	eventDispatcher.RegisterGlobalHandler(func(event cqrs.VersionedEvent) error {
		if err := integrationEventsLog.SaveIntegrationEvent(event); err != nil {
			return err
		}
		if err := readModel.UpdateViewModel([]cqrs.VersionedEvent{event}); err != nil {
			return err
		}
		if err := usersModel.UpdateViewModel([]cqrs.VersionedEvent{event}); err != nil {
			return err
		}
		return nil
	})
}

func RegisterCommandHandlers(commandDispatcher *cqrs.CommandDispatchManager, repository cqrs.EventSourcingRepository) {
	commandDispatcher.RegisterCommandHandler(CreateAccountCommand{}, func(command cqrs.Command) error {
		createAccountCommand := command.Body.(CreateAccountCommand)
		cqrs.PackageLogger().Debugf("Processing command - Create account")
		account := NewAccount(createAccountCommand.FirstName,
			createAccountCommand.LastName,
			createAccountCommand.EmailAddress,
			createAccountCommand.PasswordHash,
			createAccountCommand.InitialBalance)

		cqrs.PackageLogger().Debugf("Set ID...")
		account.SetID(accountID)
		cqrs.PackageLogger().Debugf("Persist the account")
		cqrs.PackageLogger().Debugf("Account %+v", account)

		if _, err := repository.Save(account, command.CorrelationID); err != nil {
			return err
		}
		cqrs.PackageLogger().Debugf(account.String())
		return nil
	})

	commandDispatcher.RegisterCommandHandler(ChangeEmailAddressCommand{}, func(command cqrs.Command) error {
		changeEmailAddressCommand := command.Body.(ChangeEmailAddressCommand)
		cqrs.PackageLogger().Debugf("Processing command - Change email address")
		account, err := NewAccountFromHistory(changeEmailAddressCommand.AccountID, repository)
		if err != nil {
			return err
		}

		if err := account.ChangeEmailAddress(changeEmailAddressCommand.NewEmailAddress); err != nil {
			return err
		}
		cqrs.PackageLogger().Debugf("Account %+v", account)

		cqrs.PackageLogger().Debugf("Persist the account")
		if _, err := repository.Save(account, command.CorrelationID); err != nil {
			return err
		}
		cqrs.PackageLogger().Debugf(account.String())
		return nil
	})

	commandDispatcher.RegisterCommandHandler(ChangePasswordCommand{}, func(command cqrs.Command) error {
		changePasswordCommand := command.Body.(ChangePasswordCommand)
		cqrs.PackageLogger().Debugf("Processing command - Change password")
		account, err := NewAccountFromHistory(changePasswordCommand.AccountID, repository)
		if err != nil {
			return err
		}

		cqrs.PackageLogger().Debugf("Account %+v", account)

		if err := account.ChangePassword(changePasswordCommand.NewPassword); err != nil {
			return err
		}
		cqrs.PackageLogger().Debugf("Persist the account")
		if _, err := repository.Save(account, command.CorrelationID); err != nil {
			return err
		}
		cqrs.PackageLogger().Debugf(account.String())
		return nil
	})

	commandDispatcher.RegisterCommandHandler(CreditAccountCommand{}, func(command cqrs.Command) error {
		creditAccountCommand := command.Body.(CreditAccountCommand)
		cqrs.PackageLogger().Debugf("Processing command - Credit account")
		account, err := NewAccountFromHistory(creditAccountCommand.AccountID, repository)
		if err != nil {
			return err
		}

		if err := account.Credit(creditAccountCommand.Amount); err != nil {
			return err
		}
		cqrs.PackageLogger().Debugf("Account %+v", account)

		cqrs.PackageLogger().Debugf("Persist the account")
		if _, err := repository.Save(account, command.CorrelationID); err != nil {
			return err
		}
		cqrs.PackageLogger().Debugf(account.String())
		return nil
	})

	commandDispatcher.RegisterCommandHandler(DebitAccountCommand{}, func(command cqrs.Command) error {
		debitAccountCommand := command.Body.(DebitAccountCommand)
		cqrs.PackageLogger().Debugf("Processing command - Debit account")
		account, err := NewAccountFromHistory(debitAccountCommand.AccountID, repository)
		if err != nil {
			return err
		}

		if err := account.Debit(debitAccountCommand.Amount); err != nil {
			return err
		}

		cqrs.PackageLogger().Debugf("Account %+v", account)

		cqrs.PackageLogger().Debugf("Persist the account")
		if _, err := repository.Save(account, command.CorrelationID); err != nil {
			return err
		}
		cqrs.PackageLogger().Debugf(account.String())
		return nil
	})
}
