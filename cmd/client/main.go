package main

import (
	"fmt"
	"os"

	"github.com/bootdotdev/learn-pub-sub-starter/internal/gamelogic"
	"github.com/bootdotdev/learn-pub-sub-starter/internal/routing"
	amqp "github.com/rabbitmq/amqp091-go"
)

func main() {
	fmt.Println("Starting Peril client...")

	connStr := "amqp://guest:guest@localhost:5672/"
	conn, err := amqp.Dial(connStr)
	if err != nil {
		fmt.Printf("Failed to connect to RabbitMQ: %v", err)
		os.Exit(1)
	}

	defer conn.Close()

	user, err := gamelogic.ClientWelcome()
	if err != nil {
		fmt.Printf("Error: %v\n", err)
		os.Exit(1)
	}
	queuename := routing.PauseKey + "." + user

	ch, q, err := gamelogic.DeclareAndBind(conn, routing.ExchangePerilDirect, queuename, routing.PauseKey, gamelogic.SimpleQueueTransient)
	if err != nil {
		fmt.Printf("Error declaring and binding queue: %v\n", err)
		os.Exit(1)
	}

	defer ch.Close()

	fmt.Printf("Queue: %v has been created.\n", q)

	newGame := gamelogic.NewGameState(user)

	for {
		words := gamelogic.GetInput()
		if len(words) == 0 {
			continue
		}
		cmd := words[0]

		switch cmd {
		case "spawn":
			err = newGame.CommandSpawn(words)
			if err != nil {
				fmt.Printf("Could not spawn: %v\n", err)
			}

		case "move":
			_, err := newGame.CommandMove(words)
			if err != nil {
				fmt.Printf("Could not move: %v\n", err)
			}

		case "status":
			newGame.CommandStatus()

		case "help":
			gamelogic.PrintClientHelp()

		case "spam":
			fmt.Println("Spamming not allowed yet!")

		case "quit":
			gamelogic.PrintQuit()
			return

		default:
			fmt.Printf("Unknown command: %s\n", cmd)
		}
	}

}
