package main

import (
	"fmt"
	"log"

	"github.com/cocuum/learn-pub-sub-starter/internal/pubsub"
	"github.com/cocuum/learn-pub-sub-starter/internal/routing"
	amqp "github.com/rabbitmq/amqp091-go"
)

func main() {
	const rabbitConn = "amqp://guest:guest@localhost:5672/"

	fmt.Println("Starting Peril server...")
	connect, err := amqp.Dial(rabbitConn)
	if err != nil {
		log.Fatalf("Unable to connect to rtmq: %v", err)
	}
	defer connect.Close()
	fmt.Println("rtmq connection made successfully!")

	pubCh, err := connect.Channel()
	if err != nil {
		log.Fatalf("Unable to open channel: %v:", err)
	}

	err = pubsub.PublishJSON(
		pubCh,
		routing.ExchangePerilDirect,
		routing.PauseKey,
		routing.PlayingState{IsPaused: true},
	)
	if err != nil {
		log.Fatalf("Unable to publish json to channel: %v", err)
	}

	fmt.Println("Pause message sent...")
}
