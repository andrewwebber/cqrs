CQRS framework in go
=======

[Example test scenario (inmemory)](https://github.com/andrewwebber/cqrs/blob/master/cqrs_test.go)
[Example test scenario (couchbase, rabbitmq)](https://github.com/andrewwebber/cqrs/blob/master/example/example_test.go)

## Example Bank Account Scenario
The example test scenario is of a simple bank account that seeks to track, using event sourcing, a
customers balance and login password

The are two make areas of concern at the application level, the **Write model** and **Read model**.
The read model is aimed to facilitate fast reads (read model projections)
The write model is where the business logic get executed and asynchronously notifies the read models

## Write model
### Account Aggregate
```go
type Account struct {
  cqrs.EventSourceBased

  FirstName    string
  LastName     string
  EmailAddress string
  PasswordHash []byte
  Balance      float64
}
```
To compensate for golang's lack of inheritance, a combination of type embedding and a call convention
pattern are utilized

```go
func NewAccount(firstName string, lastName string, emailAddress string, passwordHash []byte, initialBalance float64) *Account {
  account := new(Account)
  account.EventSourceBased = cqrs.NewEventSourceBased(account)

  event := AccountCreatedEvent{firstName, lastName, emailAddress, passwordHash, initialBalance}
  account.Update(event)
  return account
}
```

### Account Events
```go
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
```
Events souring events are raised using the embedded **Update** function. These events will also be published
to the read models
```go
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
```

## Read Model
### Accounts projection
```go
type ReadModelAccounts struct {
  Accounts map[string]*AccountReadModel
}

type AccountReadModel struct {
  ID           string
  FirstName    string
  LastName     string
  EmailAddress string
  Balance      float64
}
```
### Users projection
```go
type UsersModel struct {
  Users    map[string]*User
}

type User struct {
  ID           string
  FirstName    string
  LastName     string
  EmailAddress string
  PasswordHash []byte
}
```

## Infrastructure
There are three main elements to the CQRS infrastructure
- Event sourcing repository (a repository for event sourcing based business objects)
- Event publisher (publishes new events to an event bus)
- Event handler (dispatches received events to call handlers)

```go
persistance := cqrs.NewInMemoryEventStreamRepository()
bus := cqrs.NewInMemoryEventBus()
repository := cqrs.NewRepositoryWithPublisher(persistance, bus)
```

Nested packages within this repository show example implementations using Couchbase Server and RabbitMQ

With the infrastructure implementations instantiated a stock event dispatcher is provided to route received
events to call handlers
```go
readModel := NewReadModelAccounts()
usersModel := NewUsersModel()

eventDispatcher := cqrs.NewVersionedEventDispatchManager(bus)
eventDispatcher.RegisterEventHandler(AccountCreatedEvent{}, func(event cqrs.VersionedEvent) error {
  readModel.UpdateViewModel([]cqrs.VersionedEvent{event})
  usersModel.UpdateViewModel([]cqrs.VersionedEvent{event})
  return nil
})
```

Within your read models the idea is that you implement updating your pre-pared read model based upon the
incoming event notifications

The test example looks to make changes to the write side and monitor the read models becoming eventually
consistant
```go
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
```

As the read models become consistant we check at the end of the test if everything is in sync
```go
if account.EmailAddress != lastEmailAddress {
  t.Fatal("Expected emailaddress to be ", lastEmailAddress)
}

if account.Balance != readModel.Accounts[accountID].Balance {
  t.Fatal("Expected readmodel to be synced with write model")
}
```

[![Build Status](https://drone.io/github.com/andrewwebber/cqrs/status.png)](https://drone.io/github.com/andrewwebber/cqrs/latest)
