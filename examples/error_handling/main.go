package main

import (
	"context"
	"errors"
	"fmt"
	"log"
	"os"
	"strconv"
	"strings"
	"time"

	"github.com/hellocashmere/teleflow"
	"github.com/hellocashmere/teleflow/storage"
	tele "gopkg.in/telebot.v4"
)

func main() {
	pref := tele.Settings{
		Token:  os.Getenv("TOKEN"),
		Poller: &tele.LongPoller{Timeout: 10 * time.Second},
		OnError: func(err error, c tele.Context) {
			if c == nil {
				log.Printf("handler error: %v", err)
				return
			}

			log.Printf("update %d: %v", c.Update().ID, err)
		},
	}

	bot, err := tele.NewBot(pref)
	if err != nil {
		log.Fatal(err)
	}

	bus := teleflow.NewBus(storage.NewMemory())

	bot.Use(HandleErrors(
		"An internal error occurred while creating the payment. Please try again later.",
	))

	bot.Handle(tele.OnText, bus.Handle)
	bot.Handle("/payment", PaymentHandler(bus))

	log.Println("telegram bot started successfully")
	bot.Start()

	log.Println("telegram bot stopped")
}

func PaymentHandler(bus teleflow.Bus) tele.HandlerFunc {
	payment := bus.NewFlow(teleflow.FlowConfig{
		Name:        "payment",
		Version:     1,
		IdleTimeout: 15 * time.Minute,
	})

	payment.Step("amount", tele.OnText,
		func(c tele.Context) error {
			return c.Send("Enter the payment amount. Use 13 to simulate an internal error.")
		},
		func(c tele.Context, fc teleflow.Context) (teleflow.StepResult, error) {
			amount, err := strconv.ParseFloat(strings.TrimSpace(c.Text()), 64)

			if err != nil || amount <= 0 {
				if err := c.Send("Please enter a positive number."); err != nil {
					return fc.Stay(), err
				}

				return fc.Stay(), nil
			}

			if err := createPayment(amount); err != nil {
				return fc.Stay(), fmt.Errorf("create payment: %w", err)
			}

			if err := c.Send(fmt.Sprintf("Payment for $%.2f created.", amount)); err != nil {
				return fc.Stay(), err
			}

			return fc.Next(), nil
		},
	)

	return func(c tele.Context) error {
		if err := payment.Build(); err != nil {
			return fmt.Errorf("build payment flow: %w", err)
		}

		return bus.Start(context.Background(), c, payment)
	}
}

func HandleErrors(message string) tele.MiddlewareFunc {
	return func(next tele.HandlerFunc) tele.HandlerFunc {
		return func(c tele.Context) error {
			err := next(c)
			if err == nil {
				return nil
			}

			if sendErr := c.Send(message); sendErr != nil {
				return errors.Join(err, fmt.Errorf("send error message: %w", sendErr))
			}

			return err
		}
	}
}

func createPayment(amount float64) error {
	if amount == 13 {
		return errors.New("payment provider is unavailable")
	}

	return nil
}
