package pubsub

import (
	"bytes"
	"encoding/gob"
	"fmt"
	"log"

	amqp "github.com/rabbitmq/amqp091-go"
)

func SubscribeGob[T any](conn *amqp.Connection, exchange, queueName, key string, queueType SimpleQueueType, handler func(T) Acktype) error {
	ch, q, err := DeclareAndBind(conn, exchange, queueName, key, queueType)
	if err != nil {
		return err
	}

	msgs, err := ch.Consume(
		q.Name,
		"",
		false,
		false,
		false,
		false,
		nil,
	)
	if err != nil {
		ch.Close()
		return err
	}

	go func() {
		for d := range msgs {
			var buf T
			if err := gob.NewDecoder(bytes.NewReader(d.Body)).Decode(&buf); err != nil {
				log.Printf("failed to decode message: %v", err)
				continue
			}

			result := handler(buf)
			switch result {
			case Ack:
				d.Ack(false)
				fmt.Println("Ack")
			case NackRequeue:
				d.Nack(false, true)
				fmt.Println("NackRequeue")
			case NackDiscard:
				d.Nack(false, false)
				fmt.Println("NackDiscard")
			}

		}
	}()

	return nil
}
