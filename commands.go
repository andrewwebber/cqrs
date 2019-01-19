package cqrs

import (
	"reflect"
	"time"
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
	return Command{MessageID: "mid:" + NewUUIDString(),
		CorrelationID: "cid:" + NewUUIDString(),
		CommandType:   commandType.String(),
		Created:       time.Now(),

		Body: body}
}

// CreateCommandWithCorrelationID is a helper for creating a new command object with populated default properties
func CreateCommandWithCorrelationID(body interface{}, correlationID string) Command {
	commandType := reflect.TypeOf(body)
	return Command{MessageID: "mid:" + NewUUIDString(),
		CorrelationID: correlationID,
		CommandType:   commandType.String(),
		Created:       time.Now(),
		Body:          body}
}

// CommandPublisher is responsilbe for publishing commands
type CommandPublisher interface {
	PublishCommands([]Command) error
}

// CommandReceiver is responsible for receiving commands
type CommandReceiver interface {
	ReceiveCommands(CommandReceiverOptions) error
}

// CommandBus ...
type CommandBus interface {
	CommandReceiver
	CommandPublisher
}

// CommandDispatchManager is responsible for coordinating receiving messages from command receivers and dispatching them to the command dispatcher.
type CommandDispatchManager struct {
	commandDispatcher *MapBasedCommandDispatcher
	typeRegistry      TypeRegistry
	receiver          CommandReceiver
}

// CommandDispatcher the internal command dispatcher
func (m *CommandDispatchManager) CommandDispatcher() CommandDispatcher {
	return m.commandDispatcher
}

// CommandReceiverOptions is an initalization structure to communicate to and from a command receiver go routine
type CommandReceiverOptions struct {
	TypeRegistry   TypeRegistry
	Close          chan chan error
	Error          chan error
	ReceiveCommand CommandHandler
	Exclusive      bool
	ListenerCount  int
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
				metricsCommandsFailed.WithLabelValues(command.CommandType).Inc()
				return err
			}
		}
	}

	for _, handler := range m.globalHandlers {
		if err := handler(command); err != nil {
			metricsCommandsFailed.WithLabelValues(command.CommandType).Inc()
			return err
		}
	}

	metricsCommandsDispatched.WithLabelValues(command.CommandType).Inc()

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
func (m *CommandDispatchManager) Listen(stop <-chan bool, exclusive bool, listenerCount int) error {
	// Create communication channels
	//
	// for closing the queue listener,
	closeChannel := make(chan chan error)
	// receiving errors from the listener thread (go routine)
	errorChannel := make(chan error)

	// Command received channel receives a result with a channel to respond to, signifying successful processing of the message.
	// This should eventually call a command handler. See cqrs.NewVersionedCommandDispatcher()
	receiveCommandHandler := func(command Command) error {
		PackageLogger().Debugf("CommandDispatchManager.DispatchCommand: %v", command.CorrelationID)
		err := m.commandDispatcher.DispatchCommand(command)
		if err != nil {
			PackageLogger().Debugf("Error dispatching command: %v", err)
		}

		return err
	}

	// Start receiving commands by passing these channels to the worker thread (go routine)
	options := CommandReceiverOptions{m.typeRegistry, closeChannel, errorChannel, receiveCommandHandler, exclusive, listenerCount}
	if err := m.receiver.ReceiveCommands(options); err != nil {
		return err
	}
	go func() {
		for {
			// Wait on multiple channels using the select control flow.
			select {
			case <-stop:
				PackageLogger().Debugf("CommandDispatchManager.Stopping")
				closeSignal := make(chan error)
				closeChannel <- closeSignal
				PackageLogger().Debugf("CommandDispatchManager.Stopped")
				<-closeSignal
			// Receiving on this channel signifys an error has occured worker processor side
			case err := <-errorChannel:
				PackageLogger().Debugf("CommandDispatchManager.ErrorReceived: %s", err)

			}
		}
	}()

	return nil
}
