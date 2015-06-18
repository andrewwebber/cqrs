package cqrs

import (
	"log"
	"reflect"
	"time"

	"github.com/pborman/uuid"
)

// Command represents an actor intention to alter the state of the system
type Command struct {
	MessageID     string    `json:"messageID"`
	CorrelationID string    `json:"correlationID"`
	CommandType   string    `json:"commandType"`
	Created       time.Time `json:"time"`
	Body          interface{}
}

// CreateCommand is a helper for creating a new command object with populated default properties
func CreateCommand(body interface{}) Command {
	commandType := reflect.TypeOf(body)
	return Command{uuid.New(), uuid.New(), commandType.String(), time.Now(), body}
}

// CreateCommandWithCorrelationID is a helper for creating a new command object with populated default properties
func CreateCommandWithCorrelationID(body interface{}, correlationID string) Command {
	commandType := reflect.TypeOf(body)
	return Command{uuid.New(), correlationID, commandType.String(), time.Now(), body}
}

// CommandPublisher is responsilbe for publishing commands
type CommandPublisher interface {
	Publish(Command) error
}

// CommandReceiver is responsible for receiving commands
type CommandReceiver interface {
	ReceiveCommands(CommandReceiverOptions) error
}

// CommandDispatchManager is responsible for coordinating receiving messages from command receivers and dispatching them to the command dispatcher.
type CommandDispatchManager struct {
	commandDispatcher *MapBasedCommandDispatcher
	typeRegistry      TypeRegistry
	receiver          CommandReceiver
}

// CommandReceiverOptions is an initalization structure to communicate to and from a command receiver go routine
type CommandReceiverOptions struct {
	TypeRegistry   TypeRegistry
	Close          chan chan error
	Error          chan error
	ReceiveCommand chan CommandTransactedAccept
	Exclusive      bool
}

// CommandTransactedAccept is the message routed from a command receiver to the command manager.
// Sometimes command receivers designed with reliable delivery require acknowledgements after a message has been received. The success channel here allows for such acknowledgements
type CommandTransactedAccept struct {
	Command               Command
	ProcessedSuccessfully chan bool
}

// CommandDispatcher is responsible for routing commands from the command manager to call handlers responsible for processing received commands
type CommandDispatcher interface {
	DispatchCommand(Command) error
	RegisterCommandHandler(event interface{}, handler CommandHandler)
	RegisterGlobalHandler(handler CommandHandler)
}

// CommandHandler is a function that takes a command
type CommandHandler func(Command) error

// MapBasedCommandDispatcher is a simple implementation of the command dispatcher. Using a map it registered command handlers to command types
type MapBasedCommandDispatcher struct {
	registry       map[reflect.Type][]CommandHandler
	globalHandlers []CommandHandler
}

// NewMapBasedCommandDispatcher is a constructor for the MapBasedVersionedCommandDispatcher
func NewMapBasedCommandDispatcher() *MapBasedCommandDispatcher {
	registry := make(map[reflect.Type][]CommandHandler)
	return &MapBasedCommandDispatcher{registry, []CommandHandler{}}
}

// RegisterCommandHandler allows a caller to register a command handler given a command of the specified type being received
func (m *MapBasedCommandDispatcher) RegisterCommandHandler(command interface{}, handler CommandHandler) {
	commandType := reflect.TypeOf(command)
	handlers, ok := m.registry[commandType]
	if ok {
		m.registry[commandType] = append(handlers, handler)
	} else {
		m.registry[commandType] = []CommandHandler{handler}
	}
}

// RegisterGlobalHandler allows a caller to register a wildcard command handler call on any command received
func (m *MapBasedCommandDispatcher) RegisterGlobalHandler(handler CommandHandler) {
	m.globalHandlers = append(m.globalHandlers, handler)
}

// DispatchCommand executes all command handlers registered for the given command type
func (m *MapBasedCommandDispatcher) DispatchCommand(command Command) error {
	bodyType := reflect.TypeOf(command.Body)
	if handlers, ok := m.registry[bodyType]; ok {
		for _, handler := range handlers {
			if err := handler(command); err != nil {
				return err
			}
		}
	}

	for _, handler := range m.globalHandlers {
		if err := handler(command); err != nil {
			return err
		}
	}

	return nil
}

// NewCommandDispatchManager is a constructor for the CommandDispatchManager
func NewCommandDispatchManager(receiver CommandReceiver, registry TypeRegistry) *CommandDispatchManager {
	return &CommandDispatchManager{NewMapBasedCommandDispatcher(), registry, receiver}
}

// RegisterCommandHandler allows a caller to register a command handler given a command of the specified type being received
func (m *CommandDispatchManager) RegisterCommandHandler(command interface{}, handler CommandHandler) {
	m.typeRegistry.RegisterType(command)
	m.commandDispatcher.RegisterCommandHandler(command, handler)
}

// RegisterGlobalHandler allows a caller to register a wildcard command handler call on any command received
func (m *CommandDispatchManager) RegisterGlobalHandler(handler CommandHandler) {
	m.commandDispatcher.RegisterGlobalHandler(handler)
}

// Listen starts a listen loop processing channels related to new incoming events, errors and stop listening requests
func (m *CommandDispatchManager) Listen(stop <-chan bool, exclusive bool) error {
	// Create communication channels
	//
	// for closing the queue listener,
	closeChannel := make(chan chan error)
	// receiving errors from the listener thread (go routine)
	errorChannel := make(chan error)
	// and receiving commands from the queue
	receiveCommandChannel := make(chan CommandTransactedAccept)

	// Start receiving commands by passing these channels to the worker thread (go routine)
	options := CommandReceiverOptions{m.typeRegistry, closeChannel, errorChannel, receiveCommandChannel, exclusive}
	if err := m.receiver.ReceiveCommands(options); err != nil {
		return err
	}

	for {
		// Wait on multiple channels using the select control flow.
		select {
		// Command received channel receives a result with a channel to respond to, signifying successful processing of the message.
		// This should eventually call a command handler. See cqrs.NewVersionedCommandDispatcher()
		case command := <-receiveCommandChannel:
			log.Println("CommandDispatchManager.DispatchCommand: ", command.Command)
			if err := m.commandDispatcher.DispatchCommand(command.Command); err != nil {
				log.Println("Error dispatching command: ", err)
				command.ProcessedSuccessfully <- false
			} else {
				command.ProcessedSuccessfully <- true
				log.Println("CommandDispatchManager.DispatchSuccessful")
			}
		case <-stop:
			log.Println("CommandDispatchManager.Stopping")
			closeSignal := make(chan error)
			closeChannel <- closeSignal
			defer log.Println("CommandDispatchManager.Stopped")
			return <-closeSignal
		// Receiving on this channel signifys an error has occured worker processor side
		case err := <-errorChannel:
			log.Println("CommandDispatchManager.ErrorReceived: ", err)
			return err
		}
	}
}
