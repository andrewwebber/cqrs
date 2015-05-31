package rabbit

import (
	"encoding/json"
	"errors"
	"fmt"
	"log"
	"reflect"
	"time"

	"github.com/andrewwebber/cqrs"
	"github.com/streadway/amqp"
)

type RawVersionedEvent struct {
	ID            string    `json:"id"`
	CorrelationID string    `json:"correlationID"`
	SourceID      string    `json:"sourceID"`
	Version       int       `json:"version"`
	EventType     string    `json:"eventType"`
	Created       time.Time `json:"time"`
	Event         json.RawMessage
}

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
	conn, c, events, err := bus.consumeEventsQueue(options.Exclusive)
	if err != nil {
		return err
	}

	go func() {
		for {
			select {
			case ch := <-options.Close:
				defer conn.Close()
				ch <- c.Cancel(bus.name, false)
				return

			case message, more := <-events:
				if more {
					var raw RawVersionedEvent
					if err := json.Unmarshal(message.Body, &raw); err != nil {
						options.Error <- fmt.Errorf("json.Unmarshal received event: %v", err)
					} else {
						eventType, ok := options.TypeRegistry.GetTypeByName(raw.EventType)
						if !ok {
							log.Println("EventBus.Cannot find event type", raw.EventType)
							// options.Error <- errors.New("Cannot find event type " + raw.EventType)
							message.Ack(true)
						} else {
							eventValue := reflect.New(eventType)
							event := eventValue.Interface()
							if err := json.Unmarshal(raw.Event, event); err != nil {
								options.Error <- errors.New("Error deserializing event " + raw.EventType)
							} else {
								versionedEvent := cqrs.VersionedEvent{
									ID:            raw.ID,
									CorrelationID: raw.CorrelationID,
									SourceID:      raw.SourceID,
									Version:       raw.Version,
									EventType:     raw.EventType,
									Created:       raw.Created,
									Event:         reflect.Indirect(eventValue).Interface()}
								ackCh := make(chan bool)
								log.Println("EventBus.Dispatching Message")
								options.ReceiveEvent <- cqrs.VersionedEventTransactedAccept{versionedEvent, ackCh}
								result := <-ackCh
								if result {
									message.Ack(result)
								} else {
									message.Reject(true)
								}
							}
						}
					}
				} else {
					// Could have been disconnected
					log.Println("Stopped listening for messages")
					conn, c, events, err = bus.consumeEventsQueue(options.Exclusive)
				}
			}
		}
	}()

	return nil
}

func (bus *EventBus) DeleteQueue(name string) error {
	// Connects opens an AMQP connection from the credentials in the URL.
	conn, err := amqp.Dial(bus.connectionString)
	if err != nil {
		return fmt.Errorf("connection.open: %s", err)
	}

	c, err := conn.Channel()
	if err != nil {
		return fmt.Errorf("channel.open: %s", err)
	}

	_, err = c.QueueDelete(name, false, false, true)
	return err
}

func (bus *EventBus) consumeEventsQueue(exclusive bool) (*amqp.Connection, *amqp.Channel, <-chan amqp.Delivery, error) {
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

	if err := c.Qos(1, 0, false); err != nil {
		return nil, nil, nil, fmt.Errorf("Qos: %v", err)
	}

	events, err := c.Consume(bus.name, bus.name, false, exclusive, false, false, nil)
	if err != nil {
		return nil, nil, nil, fmt.Errorf("basic.consume: %v", err)
	}

	return conn, c, events, nil
}
