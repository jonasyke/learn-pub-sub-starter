package main

import (
	"fmt"

	"github.com/bootdotdev/learn-pub-sub-starter/internal/gamelogic"
	"github.com/bootdotdev/learn-pub-sub-starter/internal/pubsub"
	"github.com/bootdotdev/learn-pub-sub-starter/internal/routing"
	amqp "github.com/rabbitmq/amqp091-go"
)

func handlerPause(gs *gamelogic.GameState) func(routing.PlayingState) pubsub.Acktype {
	return func(ps routing.PlayingState) pubsub.Acktype {
		defer fmt.Print("> ")
		gs.HandlePause(ps)
		return pubsub.Ack
	}
}

func handlerMoves(gs *gamelogic.GameState, ch *amqp.Channel) func(gamelogic.ArmyMove) pubsub.Acktype {
	return func(msg gamelogic.ArmyMove) pubsub.Acktype {
		defer fmt.Print("> ")
		outcome := gs.HandleMove(msg)
		switch outcome {
		case gamelogic.MoveOutcomeSafe:
			return pubsub.Ack
		case gamelogic.MoveOutcomeMakeWar:
			err := pubsub.PublishJSON(
				ch, routing.ExchangePerilTopic, routing.WarRecognitionsPrefix+"."+gs.GetUsername(), gamelogic.RecognitionOfWar{
					Attacker: msg.Player,
					Defender: gs.GetPlayerSnap(),
				},
			)
			if err != nil {
				fmt.Printf("error: %s\n", err)
				return pubsub.NackRequeue
			}
			return pubsub.Ack
			
		default:
			return pubsub.NackDiscard
		}

	}
}

func handlerWar(gs *gamelogic.GameState) func(wr gamelogic.RecognitionOfWar) pubsub.Acktype {
	return func(wr gamelogic.RecognitionOfWar) pubsub.Acktype {
			defer fmt.Print("> ")
			outcome,_,_ := gs.HandleWar(wr)
			switch outcome {
			case gamelogic.WarOutcomeNotInvolved:
				return pubsub.NackRequeue
			case gamelogic.WarOutcomeNoUnits:
				return pubsub.NackDiscard
			case gamelogic.WarOutcomeOpponentWon:
				return pubsub.Ack
			case gamelogic.WarOutcomeYouWon:
				return pubsub.Ack
			case gamelogic.WarOutcomeDraw:
				return pubsub.Ack
			default:
				fmt.Println("error: unknown war outcome")
				return pubsub.NackDiscard
			}
	}

}
