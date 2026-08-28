package main

import (
	"fmt"

	"github.com/cocuum/learn-pub-sub-starter/internal/gamelogic"
	"github.com/cocuum/learn-pub-sub-starter/internal/pubsub"
	"github.com/cocuum/learn-pub-sub-starter/internal/routing"
)

func handlerGameLog() func(log routing.GameLog) pubsub.Acktype {
	return func(log routing.GameLog) pubsub.Acktype {
		defer fmt.Print("> ")
		err := gamelogic.WriteLog(log)
		if err != nil {
			fmt.Printf("Error writing log: %v\n", err)
			return pubsub.NackRequeue
		}
		return pubsub.Ack
	}
}
