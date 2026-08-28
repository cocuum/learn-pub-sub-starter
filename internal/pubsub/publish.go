package pubsub

import (
	"bytes"
	"context"
	"encoding/gob"
	"encoding/json"
	"fmt"

	amqp "github.com/rabbitmq/amqp091-go"
)

func PublishGob[T any](ch *amqp.Channel, exchange, key string, val T) error {
	buf := bytes.Buffer{}
	enc := gob.NewEncoder(&buf)
	err := enc.Encode(val)
	if err != nil {
		return fmt.Errorf("Unable to gob message: %v", err)
	}

	err = ch.PublishWithContext(
		context.Background(),
		exchange, key,
		false,
		false,
		amqp.Publishing{
			ContentType: "application/gob",
			Body: buf.Bytes(),
		},
	)
	if err != nil {
		return fmt.Errorf("Unable to publish to channel: %v", err)
	}
	return nil
}

func PublishJSON[T any](ch *amqp.Channel, exchange, key string, val T) error {
	data, err := json.Marshal(val)
	if err != nil {
		return fmt.Errorf("Unable to marshal message: %v", err)
	}

	err = ch.PublishWithContext(
		context.Background(),
		exchange, key,
		false,
		false,
		amqp.Publishing{
			ContentType: "application/json",
			Body: data,
		},
	)
	if err != nil {
		return fmt.Errorf("Unable to publish to channel: %v", err)
	}
	return nil
}