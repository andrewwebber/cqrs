package example_test

import (
	"encoding/json"
	"fmt"
	"github.com/andrewwebber/cqrs"
	"io/ioutil"
	"log"
	"os"
)

type User struct {
	ID           string
	FirstName    string
	LastName     string
	EmailAddress string
	PasswordHash []byte
}

func (user *User) String() string {
	return fmt.Sprintf("UserModel::User %s with Email Address %s and Password Hash %f", user.ID, user.EmailAddress, user.PasswordHash)
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

func (model *UsersModel) LoadUsers(persistance cqrs.EventStreamRepository, repository cqrs.TypeRegistry) {
	readBytes, error := ioutil.ReadFile("/tmp/users.json")

	if !os.IsNotExist(error) {
		log.Println("Loading users from disk")
		json.Unmarshal(readBytes, &model.Users)
	} else {
		log.Println("Replaying events from repository")
		events, error := persistance.Get("5058e029-d329-4c4b-b111-b042e48b0c5f", repository)
		if error == nil {
			model.UpdateViewModel(events)
		}
	}
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

	bytes, error := json.Marshal(model.Users)
	if error != nil {
		return error
	}

	error = ioutil.WriteFile("/tmp/users.json", bytes, 0644)
	if error != nil {
		return error
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
