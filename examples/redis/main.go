package main

import (
	"context"
	"fmt"
	"log"
	"os"
	"time"

	"github.com/hellocashmere/teleflow"
	"github.com/redis/go-redis/v9"
	tele "gopkg.in/telebot.v4"
)

func main() {
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	client := redis.NewClient(&redis.Options{Addr: envOr("REDIS_ADDR", "localhost:6379")})
	defer func() {
		if err := client.Close(); err != nil {
			log.Printf("close redis: %v", err)
		}
	}()

	if err := client.Ping(ctx).Err(); err != nil {
		log.Fatalf("connect to redis: %v", err)
	}

	bot, err := tele.NewBot(tele.Settings{
		Token:  os.Getenv("TOKEN"),
		Poller: &tele.LongPoller{Timeout: 10 * time.Second},
	})
	if err != nil {
		log.Fatal(err)
	}

	store := newRedisStorage(client, "teleflow:")
	bus := teleflow.NewBus(store)
	greeting := newGreetingFlow(bus)

	if err := greeting.Build(); err != nil {
		log.Fatal(err)
	}

	bot.Handle("/greet", func(c tele.Context) error {
		return bus.Start(context.Background(), c, greeting)
	})

	bot.Handle(tele.OnText, bus.Handle)

	log.Println("telegram bot started successfully")
	bot.Start()

	log.Println("telegram bot stopped")
}

func newGreetingFlow(bus teleflow.Bus) *teleflow.Flow {
	flow := bus.NewFlow(teleflow.FlowConfig{
		Name:        "redis-greeting",
		Version:     1,
		IdleTimeout: 15 * time.Minute,
	})

	flow.Step("name", tele.OnText,
		func(c tele.Context) error {
			return c.Send("What is your name?")
		},
		func(c tele.Context, fc teleflow.Context) (teleflow.StepResult, error) {
			if c.Text() == "" {
				return fc.Stay(), c.Send("Please enter a name.")
			}

			if err := fc.SetString("name", c.Text()); err != nil {
				return fc.Stay(), err
			}

			if err := c.Send(fmt.Sprintf("Hello, %s!", c.Text())); err != nil {
				return fc.Stay(), err
			}

			return fc.Next(), nil
		},
	)

	return flow
}

func envOr(name, fallback string) string {
	if value := os.Getenv(name); value != "" {
		return value
	}

	return fallback
}
