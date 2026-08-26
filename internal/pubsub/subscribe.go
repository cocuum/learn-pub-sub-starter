package pubsub

import (
	"encoding/json"
	"fmt"

	amqp "github.com/rabbitmq/amqp091-go"
)

func SubscribeJSON[T any](
	connect *amqp.Connection,
	exchange,
	queueName,
	key string,
	queueType SimpleQueueType,
	handler func(T),
) error {
	ch, q, err := DeclareAndBind(connect, exchange, queueName, key, queueType)
	if err != nil {
		return fmt.Errorf("Unable to declare and bind: %v", err)
	}

	messages, err := ch.Consume(
		q.Name,
		"",
		false,
		false,
		false,
		false,
		nil,
	)
	if err != nil {
		return fmt.Errorf("Unable to consume deliveries: %v", err)
	}

	unmarsh := func(data []byte) (T, error) {
		var target T
		err := json.Unmarshal(data, &target)
		return target, err
	}

	go func() {
		defer ch.Close()
		for message := range messages {
			target, err := unmarsh(message.Body)
			if err != nil {
				fmt.Printf("Unable to unmarshal delivery: %v\n", err)
				continue
			}
			handler(target)
			message.Ack(false)
		}
	}()
	return nil
}
