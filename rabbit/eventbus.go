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

// RawVersionedEvent ...
type RawVersionedEvent struct {
	ID            string    `json:"id"`
	CorrelationID string    `json:"correlationID"`
	SourceID      string    `json:"sourceID"`
	Version       int       `json:"version"`
	EventType     string    `json:"eventType"`
	Created       time.Time `json:"time"`
	Event         json.RawMessage
}

// EventBus  ...
type EventBus struct {
	resolver          ConnectionStringResolver
	name              string
	exchange          string
	channel           *amqp.Channel
	reconnect         chan reconnectionAttempt
	conn              *amqp.Connection
	reconnectContext  int
	healthyconnection uint32
}

// NewEventBus ...
func NewEventBus(resolver ConnectionStringResolver, name string, exchange string) *EventBus {
	bus := &EventBus{resolver: resolver, name: name, exchange: exchange, healthyconnection: 1}
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

func (bus *EventBus) getConnectionString() (string, error) {
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

// PublishEvents will publish events
func (bus *EventBus) PublishEvents(events []cqrs.VersionedEvent) error {

	for _, event := range events {
		encodedEvent, err := json.Marshal(event)
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
			Body:            encodedEvent,
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
			metricsEventsFailed.WithLabelValues(event.EventType).Inc()
			return fmt.Errorf("bus.publish: %v", err)
		}

		metricsEventsPublished.WithLabelValues(event.EventType).Inc()
	}

	return nil
}

func (bus *EventBus) connect(conn *amqp.Connection) error {

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
	err = c.ExchangeDeclare(bus.exchange, "fanout", true, false, false, false, nil)
	if err != nil {
		return fmt.Errorf("exchange.declare: %v", err)
	}

	bus.channel = c

	return nil
}

// ReceiveEvents will receive events
func (bus *EventBus) ReceiveEvents(options cqrs.VersionedEventReceiverOptions) error {
	conn := bus.conn
	listenerStart := time.Now()
	var wg sync.WaitGroup
	for n := 0; n < options.ListenerCount; n++ {
		wg.Add(1)
		go func(reconnectionChannel chan<- reconnectionAttempt) {
			reconnectionContext := 0
			c, events, err := bus.consumeEventsQueue(conn, options.Exclusive)
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

				cR, eventsR, errR := bus.consumeEventsQueue(conn, options.Exclusive)
				if errR == nil {
					c, events, _ = cR, eventsR, errR
				}
				notifyClose = conn.NotifyClose(make(chan *amqp.Error))
				atomic.CompareAndSwapUint32(&bus.healthyconnection, 0, 1)
			}

			for {
				select {
				case ch := <-options.Close:
					defer closeConnection(conn)
					ch <- c.Cancel(bus.name, false)
					return

				case <-notifyClose:
					reconnect()

				case m, more := <-events:
					if more {
						go func(message amqp.Delivery) {
							var raw RawVersionedEvent
							if errUnmarshalRaw := json.Unmarshal(message.Body, &raw); errUnmarshalRaw != nil {
								options.Error <- fmt.Errorf("json.Unmarshal received event: %v", errUnmarshalRaw)
							} else {
								eventType, ok := options.TypeRegistry.GetTypeByName(raw.EventType)
								if !ok {
									// cqrs.PackageLogger().Debugf(nil, "EventBus.Cannot find event type", raw.EventType)
									// options.Error <- errors.New("Cannot find event type " + raw.EventType)
									err = message.Ack(false)
									if err != nil {
										cqrs.PackageLogger().Debugf("ERROR: Message ack failed: %v\n", err)
									}
								} else {
									eventValue := reflect.New(eventType)
									event := eventValue.Interface()
									if errUnmarshalEvent := json.Unmarshal(raw.Event, event); errUnmarshalEvent != nil {
										options.Error <- errors.New("Error deserializing event " + raw.EventType)
									} else {
										versionedEvent := cqrs.VersionedEvent{
											ID:            raw.ID,
											CorrelationID: raw.CorrelationID,
											SourceID:      raw.SourceID,
											Version:       raw.Version,
											EventType:     raw.EventType,
											Created:       raw.Created,

											Event: reflect.Indirect(eventValue).Interface()}

										start := time.Now()
										execErr := options.ReceiveEvent(versionedEvent)
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
											// 	"CQRS_TYPE":     raw.EventType,
											// 	"CQRS_CREATED":  fmt.Sprintf("%s", raw.Created),
											// 	"CQRS_CORR":     raw.CorrelationID}
											cqrs.PackageLogger().Debugf("EventBus Message Took %s", elapsed)
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
						c, events, err = bus.consumeEventsQueue(conn, options.Exclusive)
						for err != nil {
							reconnect()
							c, events, err = bus.consumeEventsQueue(conn, options.Exclusive)
							<-time.After(1 * time.Second)
						}
					}
				}
			}
		}(bus.reconnect)
	}

	wg.Wait()
	listenerElapsed := time.Since(listenerStart)
	cqrs.PackageLogger().Debugf("Receiving events - %s", listenerElapsed)

	return nil
}

// DeleteQueue will delete a queue
func (bus *EventBus) DeleteQueue(name string) error {
	// Connects opens an AMQP connection from the credentials in the URL.
	connectionString, err := bus.getConnectionString()
	if err != nil {
		return err
	}

	conn, err := amqp.Dial(connectionString)
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

func (bus *EventBus) consumeEventsQueue(conn *amqp.Connection, exclusive bool) (*amqp.Channel, <-chan amqp.Delivery, error) {

	c, err := conn.Channel()
	if err != nil {
		return nil, nil, fmt.Errorf("channel.open: %s", err)
	}

	// We declare our topology on both the publisher and consumer to ensure they
	// are the same.  This is part of AMQP being a programmable messaging model.
	//
	// See the Channel.Consume example for the complimentary declare.
	err = c.ExchangeDeclare(bus.exchange, "fanout", true, false, false, false, nil)
	if err != nil {
		return nil, nil, fmt.Errorf("exchange.declare: %v", err)
	}

	if _, err = c.QueueDeclare(bus.name, true, false, false, false, nil); err != nil {
		return nil, nil, fmt.Errorf("queue.declare: %v", err)
	}

	if err = c.QueueBind(bus.name, bus.name, bus.exchange, false, nil); err != nil {
		return nil, nil, fmt.Errorf("queue.bind: %v", err)
	}

	events, err := c.Consume(bus.name, "", false, exclusive, false, false, nil)
	if err != nil {
		return nil, nil, fmt.Errorf("basic.consume: %v", err)
	}

	if err := c.Qos(Prefetch, 0, false); err != nil {
		return nil, nil, fmt.Errorf("Qos: %v", err)
	}

	return c, events, nil
}
