package cqrs_test

import (
	"github.com/andrewwebber/cqrs"
	"log"
	"testing"
)

func TestInMemoryEventStreamRepository(t *testing.T) {
	persistance := cqrs.NewInMemoryEventStreamRepository()
	repository := cqrs.NewRepository(persistance)

	hashedPassword, err := GetHashForPassword("$ThisIsMyPassword1")
	accountID := "5058e029-d329-4c4b-b111-b042e48b0c5f"

	if err != nil {
		t.Fatal("Error: ", err)
	}

	log.Println("Get hash for user...")

	log.Println("Create new account...")
	account := NewAccount("John", "Snow", "john.snow@cqrs.example", hashedPassword, 0.0)
	account.SetID(accountID)
	account.ChangePassword("$ThisIsANOTHERPassword")
	if err := repository.Save(account); err != nil {
		t.Fatal(err)
	}

	accountFromHistory, err := NewAccountFromHistory(accountID, repository)
	if err != nil {
		t.Fatal(err)
	}

	if string(accountFromHistory.PasswordHash) != string(account.PasswordHash) {
		t.Fatal("Expected PasswordHash to match")
	}

	if events, err := persistance.AllEventsEverPublished(); err != nil {
		t.Fatal(err)
	} else {
		log.Println(events)
	}
}
