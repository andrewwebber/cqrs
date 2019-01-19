package cqrs_test

import (
	"errors"
	"fmt"
	"reflect"

	"github.com/andrewwebber/cqrs"

	"golang.org/x/crypto/bcrypt"
)

type ChangeEmailAddressCommand struct {
	AccountID       string
	NewEmailAddress string
}

type CreateAccountCommand struct {
	FirstName      string
	LastName       string
	EmailAddress   string
	PasswordHash   []byte
	InitialBalance float64
}

type ChangePasswordCommand struct {
	AccountID   string
	NewPassword string
}

type CreditAccountCommand struct {
	AccountID string
	Amount    float64
}

type DebitAccountCommand struct {
	AccountID string
	Amount    float64
}

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

func (account *Account) CopyFrom(source interface{}) {

	account.FirstName = reflect.Indirect(reflect.ValueOf(source)).FieldByName("FirstName").Interface().(string)
	account.LastName = reflect.Indirect(reflect.ValueOf(source)).FieldByName("LastName").Interface().(string)
	account.EmailAddress = reflect.Indirect(reflect.ValueOf(source)).FieldByName("EmailAddress").Interface().(string)
	account.PasswordHash = reflect.Indirect(reflect.ValueOf(source)).FieldByName("PasswordHash").Interface().([]byte)
	account.Balance = reflect.Indirect(reflect.ValueOf(source)).FieldByName("Balance").Interface().(float64)

	/*cqrs.PackageLogger().Debugf("valueOfSource", valueOfSource)
	fieldValue := valueOfSource
	cqrs.PackageLogger().Debugf("fieldValue", fieldValue)
	fieldValueInterface := fieldValue
	cqrs.PackageLogger().Debugf("fieldValueInterface", fieldValueInterface)
	firstName := fieldValueInterface.(string)
	cqrs.PackageLogger().Debugf("firstName", firstName)*/

	/*account.LastName = reflect.Indirect(reflect.ValueOf(source).FieldByName("LastName")).Interface().(string)
	account.EmailAddress = reflect.Indirect(reflect.ValueOf(source).FieldByName("EmailAddress")).Interface().(string)
	account.PasswordHash = reflect.Indirect(reflect.ValueOf(source).FieldByName("PasswordHash")).Interface().([]byte)
	account.Balance = reflect.Indirect(reflect.ValueOf(source).FieldByName("FirstName")).Interface().(float64)*/
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

	snapshot, err := repository.GetSnapshot(id)
	if err == nil {
		cqrs.PackageLogger().Debugf("Loaded snapshot: %+v", snapshot)
		account.SetVersion(snapshot.Version())
		account.CopyFrom(snapshot)
		cqrs.PackageLogger().Debugf("Updated account: %+v ", account)
	}

	if err := repository.Get(id, account); err != nil {
		return nil, err
	}

	cqrs.PackageLogger().Debugf("Loaded account: %+v", account)

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
		return errors.New("invalid newEmailAddress length")
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
		return errors.New("invalid newPassword length")
	}

	hashedPassword, err := GetHashForPassword(newPassword)
	if err != nil {
		return (err)
	}

	account.Update(PasswordChangedEvent{hashedPassword})

	return nil
}

func GetHashForPassword(password string) ([]byte, error) {
	passwordBytes := []byte(password)
	// Hashing the password with the cost of 10
	hashedPassword, err := bcrypt.GenerateFromPassword(passwordBytes, 10)
	if err != nil {
		cqrs.PackageLogger().Debugf("Error getting password hash: ", err)
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
		return errors.New("invalid amount - negative credits not supported")
	}

	account.Update(AccountCreditedEvent{amount})

	return nil
}

func (account *Account) HandleAccountCredited(event AccountCreditedEvent) {
	account.Balance += event.Amount
}

func (account *Account) Debit(amount float64) error {
	if amount <= 0 {
		return errors.New("invalid amount - negative credits not supported")
	}

	if projection := account.Balance - amount; projection < 0 {
		return errors.New("negative balance not supported")
	}

	account.Update(AccountDebitedEvent{amount})

	return nil
}

func (account *Account) HandleAccountDebitedEvent(event AccountDebitedEvent) {
	account.Balance -= event.Amount
}
