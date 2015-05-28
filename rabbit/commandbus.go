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

// RawCommand represents an actor intention to alter the state of the system
type RawCommand struct {
	MessageID     string    `json:"messageID"`
	CorrelationID string    `json:"correlationID"`
	CommandType   string    `json:"commandType"`
	Created       time.Time `json:"time"`
	Body          json.RawMessage
}

type CommandBus struct {
	connectionString string
	name             string
	exchange         string
}

func NewCommandBus(connectionString string, name string, exchange string) *CommandBus {
	return &CommandBus{connectionString, name, exchange}
}

func (bus *CommandBus) PublishCommands(commands []cqrs.Command) error {
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
	err = c.ExchangeDeclare(bus.exchange, "topic", true, false, false, false, nil)
	if err != nil {
		return fmt.Errorf("exchange.declare: %v", err)
	}

	for _, command := range commands {
		encodedCommand, err := json.Marshal(command)
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
			Body:            encodedCommand,
		}

		err = c.Publish(bus.exchange, bus.name, true, false, msg)
		if err != nil {
			// Since publish is asynchronous this can happen if the network connection
			// is reset or if the server has run out of resources.
			return fmt.Errorf("basic.publish: %v", err)
		}
	}

	return nil
}

func (bus *CommandBus) ReceiveCommands(options cqrs.CommandReceiverOptions) error {
	conn, c, commands, err := bus.consumeCommandsQueue(options.Exclusive)
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

			case message, more := <-commands:
				if more {
					var raw RawCommand
					if err := json.Unmarshal(message.Body, &raw); err != nil {
						options.Error <- fmt.Errorf("json.Unmarshal received command: %v", err)
					} else {
						commandType, ok := options.TypeRegistry.GetTypeByName(raw.CommandType)
						if !ok {
							log.Println("CommandBus.Cannot find command type", raw.CommandType)
							options.Error <- errors.New("Cannot find command type " + raw.CommandType)
						} else {
							commandValue := reflect.New(commandType)
							commandBody := commandValue.Interface()
							if err := json.Unmarshal(raw.Body, commandBody); err != nil {
								options.Error <- errors.New("Error deserializing command " + raw.CommandType)
							} else {
								command := cqrs.Command{
									MessageID:     raw.MessageID,
									CorrelationID: raw.CorrelationID,
									CommandType:   raw.CommandType,
									Created:       raw.Created,
									Body:          reflect.Indirect(commandValue).Interface()}
								ackCh := make(chan bool)
								log.Println("CommandBus.Dispatching Message")
								options.ReceiveCommand <- cqrs.CommandTransactedAccept{command, ackCh}
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
					conn, c, commands, err = bus.consumeCommandsQueue(options.Exclusive)
				}
			}
		}
	}()

	return nil
}

func (bus *CommandBus) consumeCommandsQueue(exclusive bool) (*amqp.Connection, *amqp.Channel, <-chan amqp.Delivery, error) {
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
	err = c.ExchangeDeclare(bus.exchange, "topic", true, false, false, false, nil)
	if err != nil {
		return nil, nil, nil, fmt.Errorf("exchange.declare: %v", err)
	}

	if _, err = c.QueueDeclare(bus.name, true, false, false, false, nil); err != nil {
		return nil, nil, nil, fmt.Errorf("queue.declare: %v", err)
	}

	if err = c.QueueBind(bus.name, bus.name, bus.exchange, false, nil); err != nil {
		return nil, nil, nil, fmt.Errorf("queue.bind: %v", err)
	}

	if err := c.Qos(3, 0, false); err != nil {
		return nil, nil, nil, fmt.Errorf("Qos: %v", err)
	}

	commands, err := c.Consume(bus.name, bus.name, false, exclusive, false, false, nil)
	if err != nil {
		return nil, nil, nil, fmt.Errorf("basic.consume: %v", err)
	}

	return conn, c, commands, nil
}
