package main

import (
	"fmt"
	"os"

	amqp "github.com/rabbitmq/amqp091-go"

	"github.com/bootdotdev/learn-pub-sub-starter/internal/gamelogic"
	"github.com/bootdotdev/learn-pub-sub-starter/internal/pubsub"
	"github.com/bootdotdev/learn-pub-sub-starter/internal/routing"
)

func main() {
	connStr := "amqp://guest:guest@localhost:5672/"
	conn, err := amqp.Dial(connStr)
	if err != nil {
		fmt.Printf("Failed to connect to RabbitMQ: %v", err)
		os.Exit(1)
	}

	defer conn.Close()

	ch, err := conn.Channel()
	if err != nil {
		fmt.Printf("Failed to open a channel: %v", err)
		os.Exit(1)
	}

	topicCh, q, err := pubsub.DeclareAndBind(conn, routing.ExchangePerilTopic, routing.GameLogSlug, routing.GameLogSlug+".*", pubsub.SimpleQueueDurable)
	if err != nil {
		fmt.Printf("Error starting topic queue: %v\n", err)
		os.Exit(1)
	}
	defer topicCh.Close()

	fmt.Printf("Game log queue: %v has been created.\n", q)

	gamelogic.PrintServerHelp()

	for {
		words := gamelogic.GetInput()
		if len(words) == 0 {
			continue
		}
		cmd := words[0]
		switch cmd {
		case "pause":
			err = pubsub.PublishJSON(ch, routing.ExchangePerilDirect, routing.PauseKey, routing.PlayingState{IsPaused: true})
			if err != nil {
				fmt.Printf("Failed to publish pause message: %v", err)
			} else {
				fmt.Println("Game paused.")
			}
		case "resume":
			err = pubsub.PublishJSON(ch, routing.ExchangePerilDirect, routing.PauseKey, routing.PlayingState{IsPaused: false})
			if err != nil {
				fmt.Printf("Failed to publish resume message: %v", err)
			} else {
				fmt.Println("Game resumed.")
			}
		case "quit":
			fmt.Println("Shutting down server...")
			return
		default:
			fmt.Printf("Unknown command: %s\n", cmd)
		}
	}
}
