// Package cqrs provides a CQRS and Event Sourcing framework written in go influenced by the cqrs journey guide
//
// For a full guide visit http://gitlab.brainloop.com/pkg/cqrs
//
//  import "gitlab.brainloop.com/pkg/cqrs"
//
//  func NewAccount(firstName string, lastName string, emailAddress string, passwordHash []byte, initialBalance float64) *Account {
//    account := new(Account)
//    account.EventSourceBased = cqrs.NewEventSourceBased(account)
//
//    event := AccountCreatedEvent{firstName, lastName, emailAddress, passwordHash, initialBalance}
//    account.Update(event)
//    return account
//  }
//
package cqrs
