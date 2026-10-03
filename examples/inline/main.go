package main

import (
	"context"
	"fmt"
	"log"
	"os"
	"os/signal"
	"syscall"
	"time"

	"github.com/hellocashmere/teleflow"
	"github.com/hellocashmere/teleflow/storage"
	tele "gopkg.in/telebot.v4"
)

const callbackID = "survey"

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

	answer := tele.Btn{Unique: callbackID}

	bot.Handle(&answer, bus.HandleCtx(ctx))
	bot.Handle("/survey", SurveyHandler(ctx, bus))

	go func() {
		<-ctx.Done()
		bot.Stop()
	}()

	log.Println("telegram bot started successfully")
	bot.Start()

	log.Println("telegram bot stopped")
}

// SurveyHandler returns a command handler that builds and starts the survey flow.
func SurveyHandler(ctx context.Context, bus teleflow.Bus) tele.HandlerFunc {
	survey := bus.NewFlow(teleflow.FlowConfig{
		Name:        "survey",
		Version:     1,
		IdleTimeout: 15 * time.Minute,
	})

	survey.Step("notifications", tele.OnCallback,
		func(c tele.Context) error {
			return c.EditOrSend("Would you like to receive notifications?", menu(
				tele.Btn{
					Text:   "Yes",
					Unique: callbackID,
					Data:   "yes",
				},
				tele.Btn{
					Text:   "No",
					Unique: callbackID,
					Data:   "no",
				},
			))
		},
		func(c tele.Context, fc teleflow.Context) (teleflow.StepResult, error) {
			if err := c.Respond(); err != nil {
				return fc.Stay(), err
			}

			switch c.Callback().Data {
			case "yes":
				if err := fc.SetBool("notifications", true); err != nil {
					return fc.Stay(), err
				}
			case "no":
				if err := fc.SetBool("notifications", false); err != nil {
					return fc.Stay(), err
				}
			default:
				return fc.Stay(), nil
			}

			return fc.Next(), nil
		},
	)

	survey.Step("frequency", tele.OnCallback,
		func(c tele.Context) error {
			return c.EditOrSend("How often should we notify you?", menu(
				tele.Btn{
					Text:   "Every day",
					Unique: callbackID,
					Data:   "daily",
				},
				tele.Btn{
					Text:   "Once a week",
					Unique: callbackID,
					Data:   "weekly",
				},
			))
		},
		func(c tele.Context, fc teleflow.Context) (teleflow.StepResult, error) {
			if err := c.Respond(); err != nil {
				return fc.Stay(), err
			}

			frequency := c.Callback().Data
			if frequency != "daily" && frequency != "weekly" {
				return fc.Stay(), nil
			}

			if err := fc.SetString("frequency", frequency); err != nil {
				return fc.Stay(), err
			}

			return fc.Next(), nil
		},
	)

	survey.Step("confirm", tele.OnCallback,
		func(c tele.Context) error {
			return c.EditOrSend("Save these settings?", menu(
				tele.Btn{
					Text:   "Save",
					Unique: callbackID,
					Data:   "save",
				},
				tele.Btn{
					Text:   "Back",
					Unique: callbackID,
					Data:   "back",
				},
			))
		},
		func(c tele.Context, fc teleflow.Context) (teleflow.StepResult, error) {
			if err := c.Respond(); err != nil {
				return fc.Stay(), err
			}

			switch c.Callback().Data {
			case "back":
				return fc.Back(), nil
			case "save":
				notifications, _ := fc.GetBool("notifications")
				frequency, _ := fc.GetString("frequency")

				if err := c.Edit(fmt.Sprintf(
					"Settings saved!\n\nNotifications: %t\nFrequency: %s",
					notifications,
					frequency,
				), &tele.ReplyMarkup{}); err != nil {
					return fc.Stay(), err
				}

				return fc.Next(), nil
			default:
				return fc.Stay(), nil
			}
		},
	)

	return func(c tele.Context) error {
		if err := survey.Build(); err != nil {
			return fmt.Errorf("build survey flow: %w", err)
		}

		return bus.Start(ctx, c, survey)
	}
}

func menu(buttons ...tele.Btn) *tele.ReplyMarkup {
	markup := &tele.ReplyMarkup{}
	markup.Inline(markup.Row(buttons...))

	return markup
}
