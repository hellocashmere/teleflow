package main

import (
	"context"
	"fmt"
	"log"
	"os"
	"os/signal"
	"strconv"
	"strings"
	"syscall"
	"time"

	"github.com/hellocashmere/teleflow"
	"github.com/hellocashmere/teleflow/storage"
	tele "gopkg.in/telebot.v4"
)

func main() {
	ctx, stop := signal.NotifyContext(
		context.Background(),
		os.Interrupt,
		syscall.SIGTERM,
	)
	defer stop()

	pref := tele.Settings{
		Token:  os.Getenv("TOKEN"),
		Poller: &tele.LongPoller{Timeout: 10 * time.Second},
	}

	bot, err := tele.NewBot(pref)
	if err != nil {
		log.Fatal(err)
	}

	bus := teleflow.NewBus(storage.NewMemory())

	bot.Handle(tele.OnText, bus.Handle)
	bot.Handle("/trip", TripHandler(ctx, bus))

	go func() {
		<-ctx.Done()
		bot.Stop()
	}()

	log.Println("telegram bot started successfully")
	bot.Start()

	log.Println("telegram bot stopped")
}

// TripHandler returns a command handler that builds and starts the trip flow.
func TripHandler(ctx context.Context, bus teleflow.Bus) tele.HandlerFunc {
	trip := bus.NewFlow(teleflow.FlowConfig{
		Name:        "trip",
		Version:     1,
		IdleTimeout: 15 * time.Minute,
	})

	trip.Step("destination", tele.OnText,
		func(c tele.Context) error {
			return c.Send("Where do you want to go?")
		},
		func(c tele.Context, fc teleflow.Context) (teleflow.StepResult, error) {
			destination := strings.TrimSpace(c.Text())

			if destination == "" {
				if err := c.Send("Please enter a destination."); err != nil {
					return fc.Stay(), err
				}

				return fc.Stay(), nil
			}

			if err := fc.SetString("destination", destination); err != nil {
				return fc.Stay(), err
			}

			return fc.Next(), nil
		},
	)

	trip.Step("travelers", tele.OnText,
		func(c tele.Context) error {
			return c.Send("How many people are traveling?")
		},
		func(c tele.Context, fc teleflow.Context) (teleflow.StepResult, error) {
			travelers, err := strconv.Atoi(strings.TrimSpace(c.Text()))

			if err != nil || travelers < 1 {
				if err := c.Send("Please enter a positive whole number."); err != nil {
					return fc.Stay(), err
				}

				return fc.Stay(), nil
			}

			if err := fc.SetInt("travelers", travelers); err != nil {
				return fc.Stay(), err
			}

			return fc.Next(), nil
		},
	)

	trip.Step("budget", tele.OnText,
		func(c tele.Context) error {
			return c.Send("What is your total budget in USD?")
		},
		func(c tele.Context, fc teleflow.Context) (teleflow.StepResult, error) {
			budget, err := strconv.ParseFloat(strings.TrimSpace(c.Text()), 64)

			if err != nil || budget <= 0 {
				if err := c.Send("Please enter a positive number."); err != nil {
					return fc.Stay(), err
				}

				return fc.Stay(), nil
			}

			if err := fc.SetFloat64("budget", budget); err != nil {
				return fc.Stay(), err
			}

			return fc.Next(), nil
		},
	)

	trip.Step("flexible", tele.OnText,
		func(c tele.Context) error {
			return c.Send("Are your travel dates flexible? Reply yes or no.")
		},
		func(c tele.Context, fc teleflow.Context) (teleflow.StepResult, error) {
			var flexible bool

			switch strings.ToLower(strings.TrimSpace(c.Text())) {
			case "yes":
				flexible = true
			case "no":
				flexible = false
			default:
				if err := c.Send("Please reply with yes or no."); err != nil {
					return fc.Stay(), err
				}

				return fc.Stay(), nil
			}

			if err := fc.SetBool("flexible", flexible); err != nil {
				return fc.Stay(), err
			}

			return fc.Next(), nil
		},
	)

	trip.Step("departure", tele.OnText,
		func(c tele.Context) error {
			return c.Send("When do you want to leave? Use YYYY-MM-DD.")
		},
		func(c tele.Context, fc teleflow.Context) (teleflow.StepResult, error) {
			departure, err := time.Parse(time.DateOnly, strings.TrimSpace(c.Text()))

			if err != nil {
				if err := c.Send("Please use the YYYY-MM-DD format."); err != nil {
					return fc.Stay(), err
				}

				return fc.Stay(), nil
			}

			if err := fc.SetTime("departure", departure); err != nil {
				return fc.Stay(), err
			}

			return fc.Next(), nil
		},
	)

	trip.Step("duration", tele.OnText,
		func(c tele.Context) error {
			return c.Send("How many days will the trip last?")
		},
		func(c tele.Context, fc teleflow.Context) (teleflow.StepResult, error) {
			days, err := strconv.Atoi(strings.TrimSpace(c.Text()))

			if err != nil || days < 1 {
				if err := c.Send("Please enter a positive whole number."); err != nil {
					return fc.Stay(), err
				}

				return fc.Stay(), nil
			}

			if err := fc.SetDuration("duration", time.Duration(days)*24*time.Hour); err != nil {
				return fc.Stay(), err
			}

			destination, _ := fc.GetString("destination")
			travelers, _ := fc.GetInt("travelers")
			budget, _ := fc.GetFloat64("budget")
			flexible, _ := fc.GetBool("flexible")
			departure, _ := fc.GetTime("departure")
			duration, _ := fc.GetDuration("duration")

			err = c.Send(fmt.Sprintf(
				"Trip saved!\n\nDestination: %s\nTravelers: %d\nBudget: $%.2f\nFlexible dates: %t\nDeparture: %s\nDuration: %d days",
				destination,
				travelers,
				budget,
				flexible,
				departure.Format(time.DateOnly),
				duration/(24*time.Hour),
			))
			if err != nil {
				return fc.Stay(), err
			}

			return fc.Next(), nil
		},
	)

	return func(c tele.Context) error {
		if err := trip.Build(); err != nil {
			return fmt.Errorf("build trip flow: %w", err)
		}

		return bus.Start(ctx, c, trip)
	}
}
