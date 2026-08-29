package pubsub

import (
	"bytes"
	"encoding/gob"
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

func SubscribeGob[T any](
	connect *amqp.Connection,
	exchange,
	queueName,
	key string,
	queueType SimpleQueueType,
	handler func(T) Acktype,
) error {
	return subscribe[T](
		connect,
		exchange,
		queueName,
		key,
		queueType,
		handler,
		func(data []byte) (T, error) {
			buffer := bytes.NewBuffer(data)
			dec := gob.NewDecoder(buffer)
			var target T
			err := dec.Decode(&target)
			return target, err
		},
	)
}

func SubscribeJSON[T any](
	connect *amqp.Connection,
	exchange,
	queueName,
	key string,
	queueType SimpleQueueType,
	handler func(T) Acktype,
) error {
	return subscribe[T](
		connect,
		exchange,
		queueName,
		key,
		queueType,
		handler,
		func(data []byte) (T, error) {
			var target T
			err := json.Unmarshal(data,&target)
			return target, err
		},
	)
}

func subscribe[T any](
	connect *amqp.Connection,
	exchange,
	queueName,
	key string,
	queueType SimpleQueueType,
	handler func(T) Acktype,
	unmarsh func([]byte) (T, error),
) error {
	ch, q, err := DeclareAndBind(connect, exchange, queueName, key, queueType)
	if err != nil {
		return fmt.Errorf("Unable to declare and bind: %v", err)
	}

	err = ch.Qos(10, 0, false)
	if err != nil {
		return fmt.Errorf("Unable to set prefetch: %v", err)
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
		return fmt.Errorf("Unable to consume messages: %v", err)
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