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

	username, err := gamelogic.ClientWelcome()
	if err != nil {
		log.Fatalf("Unable to create username: %v", err)
	}

	gameState := gamelogic.NewGameState(username)

	err = pubsub.SubscribeJSON(
		connect,
		routing.ExchangePerilDirect,
		routing.PauseKey+"."+gameState.GetUsername(),
		routing.PauseKey,
		pubsub.SimpleQueueTransient,
		handlerPause(gameState),
	)
	if err != nil {
		log.Fatalf("Unable to subscribe to pause: %v", err)
	}

	err = pubsub.SubscribeJSON(
		connect,
		routing.ExchangePerilTopic,
		routing.ArmyMovesPrefix+"."+gameState.GetUsername(),
		routing.ArmyMovesPrefix+".*",
		pubsub.SimpleQueueTransient,
		handlerMove(gameState),
	)

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
		case "spawn":
			err := gameState.CommandSpawn(words)
			if err != nil {
				fmt.Println(err)
				continue
			}
		case "move":
			mv, err := gameState.CommandMove(words)
			if err != nil {
				fmt.Println(err)
				continue
			}
			err = pubsub.PublishJSON(
				pubCh,
				routing.ExchangePerilTopic,
				routing.ArmyMovesPrefix+"."+gameState.GetUsername(),
				mv,
			)
			if err != nil {
				log.Fatalf("Unable to publish move to channel: %v", err)
			}
			fmt.Print("Move published successfully!\n")
		case "status":
			gameState.CommandStatus()
		case "help":
			gamelogic.PrintClientHelp()
		case "spam":
			fmt.Println("Spamming not allowed yet!")
		case "quit":
			gamelogic.PrintQuit()
			return
		default:
			fmt.Println("Unknown Command")
		}
	}
}
