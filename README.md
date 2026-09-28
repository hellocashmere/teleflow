# teleflow

[![Go Version](https://img.shields.io/github/go-mod/go-version/hellocashmere/teleflow)](https://go.dev/)
[![Go Reference](https://pkg.go.dev/badge/github.com/hellocashmere/teleflow.svg)](https://pkg.go.dev/github.com/hellocashmere/teleflow)
[![Tests](https://github.com/hellocashmere/teleflow/actions/workflows/tests.yml/badge.svg)](https://github.com/hellocashmere/teleflow/actions/workflows/tests.yml)
[![codecov](https://codecov.io/gh/hellocashmere/teleflow/branch/main/graph/badge.svg)](https://codecov.io/gh/hellocashmere/teleflow)
[![License](https://img.shields.io/github/license/hellocashmere/teleflow)](LICENSE)

```sh
go get github.com/hellocashmere/teleflow
```

- [Overview](#overview)
- [Getting Started](#getting-started)
- [Context](#context)
- [License](#license)

# Overview

Teleflow is a package which is designed to solve the problem of dialogs using
[Telebot](https://github.com/tucnak/telebot). It provides a simple and efficient
way to receive and process both input text and is capable of handling button
clicks, location sending, contacts, etc.

# Getting Started

The following bot collects a destination and traveler count in two steps:

```go
package main

import (
	"context"
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
	}

	bot, err := tele.NewBot(pref)
	if err != nil {
		log.Fatal(err)
	}

	bus := teleflow.NewBus(storage.NewMemory())

	bot.Handle(tele.OnText, bus.Handle)
	bot.Handle("/trip", TripHandler(bus))

	log.Println("telegram bot started successfully")
	bot.Start()
}

func TripHandler(bus teleflow.Bus) tele.HandlerFunc {
	trip := bus.NewFlow(teleflow.FlowConfig{
		Name:        "trip",
		Version:     1,
		IdleTimeout: 15 * time.Minute,
	})

	trip.Step(
		"destination",
		tele.OnText,
		func(c tele.Context) error {
			return c.Send("Where do you want to go?")
		},
		func(c tele.Context, fc teleflow.Context) (teleflow.StepResult, error) {
			destination := strings.TrimSpace(c.Text())
			if destination == "" {
				return fc.Stay(), c.Send("Please enter a destination.")
			}

			if err := fc.SetString("destination", destination); err != nil {
				return fc.Stay(), err
			}

			return fc.Next(), nil
		},
	)

	trip.Step(
		"travelers",
		tele.OnText,
		func(c tele.Context) error {
			return c.Send("How many people are traveling?")
		},
		func(c tele.Context, fc teleflow.Context) (teleflow.StepResult, error) {
			travelers, err := strconv.Atoi(strings.TrimSpace(c.Text()))
			if err != nil || travelers < 1 {
				return fc.Stay(), c.Send("Please enter a positive whole number.")
			}

			if err := fc.SetInt("travelers", travelers); err != nil {
				return fc.Stay(), err
			}

			destination, _ := fc.GetString("destination")
			if err := c.Send(fmt.Sprintf(
				"Trip to %s saved for %d travelers.",
				destination,
				travelers,
			)); err != nil {
				return fc.Stay(), err
			}

			return fc.Next(), nil
		},
	)

	return func(c tele.Context) error {
		if err := trip.Build(); err != nil {
			return fmt.Errorf("build trip flow: %w", err)
		}

		return bus.Start(context.Background(), c, trip)
	}
}
```

`bus.Handle` receives updates for active flows. The command handler builds the
flow definition and starts a new session. Returning `Next` from the final step
completes the flow and removes its stored state.

Register `bus.Handle` for every Telebot event used by your steps. For example,
an inline-button flow also needs `bot.Handle(tele.OnCallback, bus.Handle)`.

# Context

Step callbacks work with two different contexts:

- `tele.Context` contains the current Telegram update and methods such as
  `Send`, `Edit`, `Text`, `Message`, and `Callback`.
- `teleflow.Context` contains the current flow state, typed data, metadata, and
  transition methods.

Use `teleflow.Context` to pass data between steps:

```go
if err := fc.SetString("company", "Acme"); err != nil {
	return fc.Stay(), err
}

company, ok := fc.GetString("company")
```

Typed methods are available for strings, booleans, integers, floating-point
numbers, durations, and times. `Delete` removes a value, while `Has` checks
whether a key exists.

The same context controls the next transition:

```go
return fc.Stay(), nil
return fc.Next(), nil
return fc.Back(), nil
return fc.Go("company_details"), nil
```

`Current` and `Step` return the active step name. `Depth` reports the history
depth, and `CanBack` reports whether `Back` can return to an earlier step. A
`teleflow.Context` belongs to one callback, but the data written through it is
persisted with the flow state.

The standard library `context.Context` is used for cancellation and deadlines.
It is passed to `Bus.Start`, `Bus.Cancel`, and storage methods. Use
`Flow.StepContext` when step work must observe cancellation:

```go
flow.StepContext(
	"company_details",
	tele.OnText,
	func(ctx context.Context, c tele.Context) error {
		return c.Send("Tell us about your company.")
	},
	func(
		ctx context.Context,
		c tele.Context,
		fc teleflow.Context,
	) (teleflow.StepResult, error) {
		if err := ctx.Err(); err != nil {
			return fc.Stay(), err
		}

		return fc.Next(), nil
	},
)
```

An omitted `IdleTimeout` uses `teleflow.DefaultIdleTimeout` of 24 hours. An
omitted `HistoryLimit` uses `teleflow.DefaultHistoryLimit` of 64 entries.

# License

Teleflow is distributed under the [MIT License](LICENSE).
