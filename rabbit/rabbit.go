package rabbit

import (
	"encoding/json"
	"fmt"
	"github.com/andrewwebber/cqrs"
	"github.com/streadway/amqp"
	"log"
	"time"
)

type EventBus struct {
	connectionString string
	name             string
	exchange         string
}

func NewEventBus(connectionString string, name string, exchange string) *EventBus {
	return &EventBus{connectionString, name, exchange}
}

func (bus *EventBus) PublishEvents(events []cqrs.VersionedEvent) error {
	// Connects opens an AMQP connection from the credentials in the URL.
	conn, err := amqp.Dial(bus.connectionString)
	if err != nil {
		return fmt.Errorf("connection.open: %s", err)
	}

	// This waits for a server acknowledgment which means the sockets will have
	// flushed all outbound publishings prior to returning.  It's important to
	// block on Close to not lose any publishings.
	defer conn.Close()

	c, err := conn.Channel()
	if err != nil {
		return fmt.Errorf("channel.open: %s", err)
	}

	// We declare our topology on both the publisher and consumer to ensure they
	// are the same.  This is part of AMQP being a programmable messaging model.
	//
	// See the Channel.Consume example for the complimentary declare.
	err = c.ExchangeDeclare(bus.exchange, "fanout", true, false, false, false, nil)
	if err != nil {
		return fmt.Errorf("exchange.declare: %v", err)
	}

	for _, event := range events {
		encodedEvent, err := json.Marshal(event)
		if err != nil {
			return fmt.Errorf("json.Marshal: %v", err)
		}

		// Prepare this message to be persistent.  Your publishing requirements may
		// be different.
		msg := amqp.Publishing{
			DeliveryMode:    amqp.Persistent,
			Timestamp:       time.Now(),
			ContentEncoding: "UTF-8",
			ContentType:     "text/plain",
			Body:            encodedEvent,
		}

		err = c.Publish(bus.exchange, "", true, false, msg)
		if err != nil {
			// Since publish is asynchronous this can happen if the network connection
			// is reset or if the server has run out of resources.
			return fmt.Errorf("basic.publish: %v", err)
		}
	}

	return nil
}

func (bus *EventBus) ReceiveEvents(options cqrs.VersionedEventReceiverOptions) error {

	conn, c, events, err := bus.consumeEventsQueue()

	go func() {
		select {
		case ch := <-options.Close:
			defer conn.Close()
			if err = c.Cancel(bus.name, false); err != nil {
				ch <- err
			}

		case event, more := <-events:
			log.Println("Received event: ", event)
			if more {
				var versionedEvent cqrs.VersionedEvent
				if err := json.Unmarshal(event.Body, &versionedEvent); err != nil {
					options.Error <- fmt.Errorf("json.Unmarshal received event: %v", err)
				} else {
					ackCh := make(chan bool)
					options.ReceiveEvent <- cqrs.VersionedEventTransactedAccept{versionedEvent, ackCh}
					result := <-ackCh
					event.Ack(result)
				}
			} else {
				// Could have been disconnected
				log.Println("Stopped listening for messages")
				conn, c, events, err = bus.consumeEventsQueue()
			}
		}
	}()

	return nil
}

func (bus *EventBus) consumeEventsQueue() (*amqp.Connection, *amqp.Channel, <-chan amqp.Delivery, error) {
	// Connects opens an AMQP connection from the credentials in the URL.
	conn, err := amqp.Dial(bus.connectionString)
	if err != nil {
		return nil, nil, nil, fmt.Errorf("connection.open: %s", err)
	}

	c, err := conn.Channel()
	if err != nil {
		return nil, nil, nil, fmt.Errorf("channel.open: %s", err)
	}

	// We declare our topology on both the publisher and consumer to ensure they
	// are the same.  This is part of AMQP being a programmable messaging model.
	//
	// See the Channel.Consume example for the complimentary declare.
	err = c.ExchangeDeclare(bus.exchange, "fanout", true, false, false, false, nil)
	if err != nil {
		return nil, nil, nil, fmt.Errorf("exchange.declare: %v", err)
	}

	if _, err = c.QueueDeclare(bus.name, true, false, false, false, nil); err != nil {
		return nil, nil, nil, fmt.Errorf("queue.declare: %v", err)
	}

	if err = c.QueueBind(bus.name, bus.name, bus.exchange, false, nil); err != nil {
		return nil, nil, nil, fmt.Errorf("queue.bind: %v", err)
	}

	events, err := c.Consume(bus.name, bus.name, false, true, false, false, nil)
	if err != nil {
		return nil, nil, nil, fmt.Errorf("basic.consume: %v", err)
	}

	if err := c.Qos(1, 0, false); err != nil {
		return nil, nil, nil, fmt.Errorf("Qos: %v", err)
	}

	return conn, c, events, nil
}
