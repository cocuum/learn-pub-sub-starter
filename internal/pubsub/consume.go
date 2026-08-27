package pubsub

import (
	"encoding/json"
	"fmt"

	"github.com/cocuum/learn-pub-sub-starter/internal/routing"
	amqp "github.com/rabbitmq/amqp091-go"
)

type Acktype int

type SimpleQueueType int

const (
	SimpleQueueDurable SimpleQueueType = iota
	SimpleQueueTransient
)

const (
	Ack Acktype = iota
	NackDiscard
	NackRequeue
)

func SubscribeJSON[T any](
	connect *amqp.Connection,
	exchange,
	queueName,
	key string,
	queueType SimpleQueueType,
	handler func(T) Acktype,
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
			switch handler(target) {
			case Ack:
				message.Ack(false)
			case NackRequeue:
				message.Nack(false, true)
			case NackDiscard:
				message.Nack(false, false)
			}
		}
	}()
	return nil
}

func DeclareAndBind(
	conn *amqp.Connection, 
	exchange,
	queueName,
	key string,
	queueType SimpleQueueType,
	)(*amqp.Channel,amqp.Queue, error) {
		ch, err := conn.Channel()
		if err != nil {
			return nil,amqp.Queue{}, fmt.Errorf("Unable to create channel: %v", err)
		}

		q, err := ch.QueueDeclare(
			queueName,
			queueType == SimpleQueueDurable,
			queueType != SimpleQueueDurable,
			queueType != SimpleQueueDurable,
			false,
			amqp.Table{
				"x-dead-letter-exchange": routing.ExchangePerilDeadLetter,
			},
		)
		if err != nil {
			return nil,amqp.Queue{}, fmt.Errorf("Unable to declare queue: %v", err)
		}

		err = ch.QueueBind(
			q.Name,
			key,
			exchange,
			false,
			nil)
		if err != nil {
			return nil,amqp.Queue{}, fmt.Errorf("Unable to bind queue: %v", err)
		}

		return ch,q,nil
	}