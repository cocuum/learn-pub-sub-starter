package main

import (
	"fmt"
	"log"

	"github.com/cocuum/learn-pub-sub-starter/internal/gamelogic"
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

	gamelogic.PrintClientHelp()

	for {
		words := gamelogic.GetInput()
		if len(words) == 0 {
			continue
		}

		switch words[0] {
		case "pause":
			fmt.Println("Sending pause message...")

			err = pubsub.PublishJSON(
				pubCh,
				routing.ExchangePerilDirect,
				routing.PauseKey,
				routing.PlayingState{IsPaused: true},
			)
			if err != nil {
				log.Fatalf("Unable to publish pause to channel: %v", err)
			}
		case "resume":
			fmt.Println("Sending resume message...")

			err = pubsub.PublishJSON(
				pubCh,
				routing.ExchangePerilDirect,
				routing.PauseKey,
				routing.PlayingState{IsPaused: false},
			)
			if err != nil {
				log.Fatalf("Unable to publish resume to channel: %v", err)
			}
		case "quit":
			fmt.Println("Exiting...")
			return
		default:
			fmt.Println("Unable to understand command")
		}
	}
}
