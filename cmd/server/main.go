package main

import (
	"fmt"
	"os"
	"os/signal"

	amqp "github.com/rabbitmq/amqp091-go"

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

	err = pubsub.PublishJSON(ch, routing.ExchangePerilDirect, routing.PauseKey, routing.PlayingState{IsPaused: true})

	if err != nil {
		fmt.Printf("Failed to publish message: %v", err)
		os.Exit(1)
	}

	defer ch.Close()

	fmt.Println("Successfully connected to RabbitMQ")

	signalChan := make(chan os.Signal, 1)
	signal.Notify(signalChan, os.Interrupt)

	fmt.Println("Press Ctrl + C to shut down...")

	<-signalChan

	fmt.Println("\nShutting Down Connection...")
}
