package main

import (
	"fmt"
	"log"
	//"os"
	//"os/signal"

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

	_, q, err := pubsub.DeclareAndBind(
		connect,
		routing.ExchangePerilDirect,
		routing.PauseKey+"."+username,
		routing.PauseKey,
		pubsub.SimpleQueueTransient,
	)
	if err != nil {
		log.Fatalf("Unable to declare and bind: %v", err)
	}

	fmt.Printf("Queue %v declared and bound!\n", q.Name)

	gameState := gamelogic.NewGameState(username)

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
			_, err := gameState.CommandMove(words)
			if err != nil {
				fmt.Println(err)
				continue
			}
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
/*
	// wait for ctrl+c
	signalChan := make(chan os.Signal, 1)
	signal.Notify(signalChan, os.Interrupt)
	<-signalChan
	fmt.Println("\nrtmq connection shut...")
*/

}
