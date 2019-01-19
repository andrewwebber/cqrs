package cqrs

// InMemoryCommandBus provides an inmemory implementation of the CommandPublisher CommandReceiver interfaces
type InMemoryCommandBus struct {
	publishedCommandsChannel chan Command
	startReceiving           bool
}

// NewInMemoryCommandBus constructor
func NewInMemoryCommandBus() *InMemoryCommandBus {
	publishedCommandsChannel := make(chan Command, 0)
	return &InMemoryCommandBus{publishedCommandsChannel, false}
}

// PublishCommands publishes Commands to the Command bus
func (bus *InMemoryCommandBus) PublishCommands(commands []Command) error {
	for _, command := range commands {
		bus.publishedCommandsChannel <- command
	}

	return nil
}

// ReceiveCommands starts a go routine that monitors incoming Commands and routes them to a receiver channel specified within the options
func (bus *InMemoryCommandBus) ReceiveCommands(options CommandReceiverOptions) error {
	go func() {
		for {
			select {
			case ch := <-options.Close:
				ch <- nil
			case command := <-bus.publishedCommandsChannel:
				err := options.ReceiveCommand(command)
				if err != nil {

				}
			}
		}
	}()

	return nil
}
