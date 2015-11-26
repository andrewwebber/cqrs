package postgres_test

import (
	"testing"

	"github.com/andrewwebber/cqrs"
	"github.com/andrewwebber/cqrs/postgres"
)

func TestPgEventStreamRepoSave(t *testing.T) {
	typeRegistry := cqrs.NewTypeRegistry()
	typeRegistry.RegisterEvents(
		AccountCreatedEvent{},
		EmailAddressChangedEvent{},
		AccountCreditedEvent{},
		AccountDebitedEvent{},
		PasswordChangedEvent{},
	)
	persistance, err := postgres.NewEventStreamRepository(
		"user=postgres password=admin dbname=cqrs_postgres_test sslmode=disable",
		typeRegistry,
	)
	if err != nil {
		t.Fatal(err)
	}

	// clear database
	persistance.GetDb().Exec("TRUNCATE TABLE events")
	persistance.GetDb().Exec("TRUNCATE TABLE events_integration")
	persistance.GetDb().Exec("TRUNCATE TABLE events_correlation")

	repository := cqrs.NewRepository(persistance, typeRegistry)
	hashedPassword, err := GetHashForPassword("$ThisIsMyPassword1")
	accountID := "5058e029-d329-4c4b-b111-b042e48b0c5f"

	account := NewAccount("John", "Snow", "john.snow@cqrs.example", hashedPassword, 0.0)
	account.SetID(accountID)
	account.ChangePassword("$ThisIsANOTHERPassword")
	if err := repository.Save(account, "correlationID"); err != nil {
		t.Fatal(err)
	}

	accountFromHistory, err := NewAccountFromHistory(accountID, repository)
	if err != nil {
		t.Fatal(err)
	}

	if string(accountFromHistory.PasswordHash) != string(account.PasswordHash) {
		t.Fatal("Expected PasswordHash to match")
	}

	if events, err := persistance.AllIntegrationEventsEverPublished(); err != nil {
		t.Fatal(err)
	} else if len(events) != 2 {
		t.Fatal("Expected two events: AccountCreatedEvent, PasswordChangedEvent")
	}

	correlationEvents, err := persistance.GetIntegrationEventsByCorrelationID("correlationID")
	if err != nil {
		t.Fatal(err)
	}

	if len(correlationEvents) == 0 {
		t.Fatal("Expeced correlation events")
	}
}
