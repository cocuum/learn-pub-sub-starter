package pubsub

import (
	"fmt"

	amqp "github.com/rabbitmq/amqp091-go"
)

type Acktype int

type SimpleQueueType int

const (
	SimpleQueueDurable SimpleQueueType = iota
	SimpleQueueTransient
)

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
			nil,
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