package main

import (
	"fmt"
	"os"
	"os/signal"

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

	signalchan := make(chan os.Signal, 1)
	signal.Notify(signalchan, os.Interrupt)

	fmt.Println("Press Ctrl + C to shut down...")

	<-signalchan

	fmt.Println("\nShutting Down Connection...")

}
