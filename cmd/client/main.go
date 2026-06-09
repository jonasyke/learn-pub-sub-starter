package main

import (
	"fmt"
	"os"

	"github.com/bootdotdev/learn-pub-sub-starter/internal/gamelogic"
	"github.com/bootdotdev/learn-pub-sub-starter/internal/pubsub"
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

	publishCH, err := conn.Channel()
	if err != nil {
		fmt.Printf("Failed to open a channel: %v\n", err)
		os.Exit(1)
	}

	defer conn.Close()

	user, err := gamelogic.ClientWelcome()
	if err != nil {
		fmt.Printf("Error: %v\n", err)
		os.Exit(1)
	}
	queuename := routing.PauseKey + "." + user

	newGame := gamelogic.NewGameState(user)

	err = pubsub.SubscribeJSON(
		conn,
		routing.ExchangePerilDirect,
		queuename,
		routing.PauseKey,
		pubsub.SimpleQueueTransient,
		handlerPause(newGame),
	)
	if err != nil {
		fmt.Printf("Failed to subscribe to pause messages: %v\n", err)
		os.Exit(1)
	}

	err = pubsub.SubscribeJSON(
		conn,
		routing.ExchangePerilTopic,
		routing.ArmyMovesPrefix+"."+user,
		routing.ArmyMovesPrefix+".*",
		pubsub.SimpleQueueTransient,
		handlerMoves(newGame, publishCH),
	)
	if err != nil {
		fmt.Printf("Failed to subscribe to army moves messages: %v\n", err)
		os.Exit(1)
	}

	err = pubsub.SubscribeJSON(
		conn,
		routing.ExchangePerilTopic,
		"war",
		routing.WarRecognitionsPrefix+".*",
		pubsub.SimpleQueueDurable,
		handlerWar(newGame, publishCH),
	)

	if err != nil {
		fmt.Printf("Failed to subscribe to war messages: %v\n", err)
		os.Exit(1)
	}

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
			move, err := newGame.CommandMove(words)
			if err != nil {
				fmt.Printf("Could not move: %v\n", err)
				continue
			}

			err = pubsub.PublishJSON(
				publishCH,
				routing.ExchangePerilTopic,
				routing.ArmyMovesPrefix+"."+user,
				move,
			)
			if err != nil {
				fmt.Printf("Failed to publish move: %v\n", err)
				continue
			}
			fmt.Printf("%s moved %d units to %s\n", move.Player.Username, len(move.Units), move.ToLocation)

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
