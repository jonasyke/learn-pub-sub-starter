package main

import (
	"fmt"

	"github.com/bootdotdev/learn-pub-sub-starter/internal/gamelogic"
	"github.com/bootdotdev/learn-pub-sub-starter/internal/pubsub"
	"github.com/bootdotdev/learn-pub-sub-starter/internal/routing"
)

func handlerLogs() func(rg routing.GameLog) pubsub.Acktype {
	return func(rg routing.GameLog) pubsub.Acktype {
		defer fmt.Print("> ")
		err := gamelogic.WriteLog(rg)
		if err != nil {
			fmt.Printf("error: %s", err)
			return pubsub.NackRequeue
		}
		return pubsub.Ack
	}
}
