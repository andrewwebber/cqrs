package rabbit

import (
	"github.com/andrewwebber/cqrs"
	"github.com/streadway/amqp"
)

type reconnectionAttempt struct {
	context  int
	response chan reconnectionAttemptResponse
}

type reconnectionAttemptResponse struct {
	connection *amqp.Connection
	newContext int
}

type reconnectedHandler func(*amqp.Connection, int)

func initializeReconnectionManagement(commString ConnectionStringResolver, reconnected reconnectedHandler) chan reconnectionAttempt {
	reconnectCh := make(chan reconnectionAttempt)
	go func(receive <-chan reconnectionAttempt) {
		initialContext := 0
		cqrs.PackageLogger().Debugf("Initial context %v", initialContext)
		var conn *amqp.Connection
		var err error
		var connectionString string
		for reconnectIssued := range receive {
			if reconnectIssued.context == initialContext {
				cqrs.PackageLogger().Debugf("Starting rabbitmq reconnection")
				retryErr := exponential(func() error {
					connectionString, err = commString()
					cqrs.PackageLogger().Debugf("Connecting to rabbitmq %v", connectionString)
					if err != nil {
						return err
					}

					conn, err = amqp.Dial(connectionString)
					return err
				}, 10)

				for retryErr != nil {
					retryErr = exponential(func() error {
						connectionString, err = commString()
						if err != nil {
							return err
						}

						conn, err = amqp.Dial(connectionString)
						return err
					}, 10)
				}
				initialContext = initialContext + 1
				cqrs.PackageLogger().Debugf("Rabbitmq reconnection successfull")
				reconnected(conn, initialContext)
			}

			reconnectIssued.response <- reconnectionAttemptResponse{connection: conn, newContext: initialContext}
		}
	}(reconnectCh)
	return reconnectCh
}
