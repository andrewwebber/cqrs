package rabbit

import (
	"encoding/json"
	"errors"
	"fmt"
	"reflect"
	"sync"
	"sync/atomic"
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

// CommandBus ...
type CommandBus struct {
	resolver          ConnectionStringResolver
	name              string
	exchange          string
	channel           *amqp.Channel
	reconnect         chan reconnectionAttempt
	conn              *amqp.Connection
	reconnectContext  int
	healthyconnection uint32
}

// NewCommandBus will create a new command bus
func NewCommandBus(resolver ConnectionStringResolver, name string, exchange string) *CommandBus {
	bus := &CommandBus{resolver: resolver, name: name, exchange: exchange, healthyconnection: 1}
	reconnectCh := initializeReconnectionManagement(resolver, func(conn *amqp.Connection, ctx int) {
		bus.conn = conn
		bus.reconnectContext = ctx
		_ = bus.connect(bus.conn)
	})
	respCh := make(chan reconnectionAttemptResponse)
	reconnectCh <- reconnectionAttempt{context: 0, response: respCh}
	<-respCh
	bus.reconnect = reconnectCh

	return bus
}

func (bus *CommandBus) getConnectionString() (string, error) {
	var connectionString string
	retryError := exponential(func() error {
		result, err := bus.resolver()
		if err != nil {
			return err
		}

		connectionString = result
		return err
	}, 5)

	return connectionString, retryError
}

// PublishCommands will publish commands
func (bus *CommandBus) PublishCommands(commands []cqrs.Command) error {

	for _, command := range commands {
		encodedCommand, err := json.Marshal(command)
		if err != nil {
			return fmt.Errorf("json.Marshal: %v", err)
		}

		// Prepare this message to be persistent.  Your publishing requirements may
		// be different.
		msg := amqp.Publishing{
			DeliveryMode:    amqp.Persistent,
			Timestamp:       time.Now().UTC(),
			ContentEncoding: "UTF-8",
			ContentType:     "text/plain",
			Body:            encodedCommand,
		}

		retryError := exponential(func() error {
			err = bus.channel.Publish(bus.exchange, bus.name, true, false, msg)

			if err != nil {
				atomic.CompareAndSwapUint32(&bus.healthyconnection, 1, 0)
				respCh := make(chan reconnectionAttemptResponse)
				bus.reconnect <- reconnectionAttempt{context: bus.reconnectContext, response: respCh}
				resp := <-respCh
				bus.conn = resp.connection
				bus.reconnectContext = resp.newContext

				connErr := bus.connect(bus.conn)
				if connErr == nil {
					cqrs.PackageLogger().Debugf("RabbitMQ: Reconnected")
					atomic.CompareAndSwapUint32(&bus.healthyconnection, 0, 1)
				} else {
					cqrs.PackageLogger().Debugf("RabbitMQ: Reconnect Failed %v", err)
				}
			}

			return err
		}, 3)

		if retryError != nil {
			metricsCommandsFailed.WithLabelValues(command.CommandType).Inc()
			return fmt.Errorf("bus.publish: %v", err)
		}

		metricsCommandsPublished.WithLabelValues(command.CommandType).Inc()
	}

	return nil
}

func (bus *CommandBus) connect(conn *amqp.Connection) error {
	// This waits for a server acknowledgment which means the sockets will have
	// flushed all outbound publishings prior to returning.  It's important to
	// block on Close to not lose any publishings.
	//defer conn.Close()

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

	bus.channel = c

	return nil
}

// ReceiveCommands will recieve commands
func (bus *CommandBus) ReceiveCommands(options cqrs.CommandReceiverOptions) error {
	conn := bus.conn

	listenerStart := time.Now()
	var wg sync.WaitGroup
	for n := 0; n < options.ListenerCount; n++ {
		wg.Add(1)
		go func(reconnectionChannel chan<- reconnectionAttempt) {
			reconnectionContext := 0
			c, commands, err := bus.consumeCommandsQueue(conn, options.Exclusive)
			if err != nil {
				return
			}

			wg.Done()

			notifyClose := conn.NotifyClose(make(chan *amqp.Error))
			reconnect := func() {
				atomic.CompareAndSwapUint32(&bus.healthyconnection, 1, 0)
				respCh := make(chan reconnectionAttemptResponse)
				reconnectionChannel <- reconnectionAttempt{context: reconnectionContext, response: respCh}
				resp := <-respCh
				conn = resp.connection
				reconnectionContext = resp.newContext

				cR, commandsR, errR := bus.consumeCommandsQueue(conn, options.Exclusive)
				if errR == nil {
					c, commands, _ = cR, commandsR, errR
				}

				notifyClose = conn.NotifyClose(make(chan *amqp.Error))
				atomic.CompareAndSwapUint32(&bus.healthyconnection, 0, 1)
			}

			for {
				select {
				case ch := <-options.Close:
					cqrs.PackageLogger().Debugf("Close requested")
					defer closeConnection(conn)
					ch <- c.Cancel(bus.name, false)
					return

				case <-notifyClose:
					reconnect()

				case m, more := <-commands:
					if more {
						go func(message amqp.Delivery) {
							var raw RawCommand
							if errUnmarshalRaw := json.Unmarshal(message.Body, &raw); errUnmarshalRaw != nil {
								options.Error <- fmt.Errorf("json.Unmarshal received command: %v", errUnmarshalRaw)
							} else {
								commandType, ok := options.TypeRegistry.GetTypeByName(raw.CommandType)
								if !ok {
									cqrs.PackageLogger().Debugf("CommandBus.Cannot find command type", raw.CommandType)
									options.Error <- errors.New("Cannot find command type " + raw.CommandType)
								} else {
									commandValue := reflect.New(commandType)
									commandBody := commandValue.Interface()
									if errUnmarshalBody := json.Unmarshal(raw.Body, commandBody); errUnmarshalBody != nil {
										options.Error <- errors.New("Error deserializing command " + raw.CommandType)
									} else {
										command := cqrs.Command{
											MessageID:     raw.MessageID,
											CorrelationID: raw.CorrelationID,
											CommandType:   raw.CommandType,
											Created:       raw.Created,

											Body: reflect.Indirect(commandValue).Interface()}

										start := time.Now()
										execErr := options.ReceiveCommand(command)
										result := execErr == nil
										if result {
											err = message.Ack(false)
											if err != nil {
												cqrs.PackageLogger().Debugf("ERROR: Message ack returned error: %v\n", err)
											}
											elapsed := time.Since(start)
											// stats := map[string]string{
											// 	"CQRS_LOG":      "true",
											// 	"CQRS_DURATION": fmt.Sprintf("%s", elapsed),
											// 	"CQRS_TYPE":     raw.CommandType,
											// 	"CQRS_CREATED":  fmt.Sprintf("%s", raw.Created),
											// 	"CQRS_CORR":     raw.CorrelationID}
											cqrs.PackageLogger().Debugf(fmt.Sprintf("CommandBus Message Took %s", elapsed))
										} else {
											err = message.Reject(true)
											if err != nil {
												cqrs.PackageLogger().Debugf("ERROR: Message reject returned error: %v\n", err)
											}
										}
									}
								}
							}
						}(m)
					} else {
						c, commands, _ = bus.consumeCommandsQueue(conn, options.Exclusive)
						for err != nil {
							reconnect()
							c, commands, _ = bus.consumeCommandsQueue(conn, options.Exclusive)
							<-time.After(1 * time.Second)
						}
					}
				}
			}
		}(bus.reconnect)
	}

	wg.Wait()
	listenerElapsed := time.Since(listenerStart)
	cqrs.PackageLogger().Debugf("Receiving commands - %s", listenerElapsed)

	return nil
}

func (bus *CommandBus) consumeCommandsQueue(conn *amqp.Connection, exclusive bool) (*amqp.Channel, <-chan amqp.Delivery, error) {

	c, err := conn.Channel()
	if err != nil {
		return nil, nil, fmt.Errorf("channel.open: %s", err)
	}

	// We declare our topology on both the publisher and consumer to ensure they
	// are the same.  This is part of AMQP being a programmable messaging model.
	//
	// See the Channel.Consume example for the complimentary declare.
	err = c.ExchangeDeclare(bus.exchange, "topic", true, false, false, false, nil)
	if err != nil {
		return nil, nil, fmt.Errorf("exchange.declare: %v", err)
	}

	if _, err = c.QueueDeclare(bus.name, true, false, false, false, nil); err != nil {
		return nil, nil, fmt.Errorf("queue.declare: %v", err)
	}

	if err = c.QueueBind(bus.name, bus.name, bus.exchange, false, nil); err != nil {
		return nil, nil, fmt.Errorf("queue.bind: %v", err)
	}

	commands, err := c.Consume(bus.name, "", false, exclusive, false, false, nil)
	if err != nil {
		return nil, nil, fmt.Errorf("basic.consume: %v", err)
	}

	if err := c.Qos(Prefetch, 0, false); err != nil {
		return nil, nil, fmt.Errorf("Qos: %v", err)
	}

	return c, commands, nil
}

func closeConnection(conn *amqp.Connection) {
	err := conn.Close()
	if err != nil {
		cqrs.PackageLogger().Debugf("Couldn't close conn: %v\n", err)
	}
}
