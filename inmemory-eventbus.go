package cqrs

type InMemoryEventBus struct {
	publishedEventsChannel chan VersionedEvent
	startReceiving         bool
}

func NewInMemoryEventBus() *InMemoryEventBus {
	publishedEventsChannel := make(chan VersionedEvent, 0)
	return &InMemoryEventBus{publishedEventsChannel, false}
}

func (bus *InMemoryEventBus) PublishEvents(events []VersionedEvent) error {
	if !bus.startReceiving {
		return nil
	}

	for _, event := range events {
		bus.publishedEventsChannel <- event
	}

	return nil
}

func (bus *InMemoryEventBus) ReceiveEvents(options VersionedEventReceiverOptions) error {
	bus.startReceiving = true

	go func() {
		for {
			select {
			case ch := <-options.Close:
				ch <- nil
			case versionedEvent := <-bus.publishedEventsChannel:
				ackCh := make(chan bool)
				options.ReceiveEvent <- VersionedEventTransactedAccept{versionedEvent, ackCh}
				<-ackCh
			}
		}
	}()

	return nil
}
