# teleflow

[![Go Version](https://img.shields.io/github/go-mod/go-version/hellocashmere/teleflow)](https://go.dev/)
[![Go Reference](https://pkg.go.dev/badge/github.com/hellocashmere/teleflow.svg)](https://pkg.go.dev/github.com/hellocashmere/teleflow)
[![Tests](https://github.com/hellocashmere/teleflow/actions/workflows/tests.yml/badge.svg)](https://github.com/hellocashmere/teleflow/actions/workflows/tests.yml)
[![codecov](https://codecov.io/gh/hellocashmere/teleflow/branch/main/graph/badge.svg)](https://codecov.io/gh/hellocashmere/teleflow)
[![License](https://img.shields.io/github/license/hellocashmere/teleflow)](LICENSE)

- [Overview](#overview)
- [Installation](#installation)
- [Getting Started](#getting-started)
- [Context](#context)
- [License](#license)

## Overview

Teleflow is a conversation-flow package for [Telebot](https://github.com/tucnak/telebot).
It defines ordered, versioned steps, persists session state through a small storage interface, and routes text, callbacks, media, and other Telebot events to active flows.
Session state includes typed data and bounded step history.

## Installation

```sh
go get github.com/hellocashmere/teleflow
```

## Getting Started

The following bot collects a destination and traveler count in two steps:

```go
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
}

func TripHandler(ctx context.Context, bus teleflow.Bus) tele.HandlerFunc {
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

		return bus.Start(ctx, c, trip)
	}
}
```

`bus.Handle` receives updates for active flows using a background context.
The command handler builds the flow definition before calling `Start`.
`Start` creates a session when none exists, resumes a pending step-entry callback, or leaves an active session unchanged.
Returning `Next` from the final step completes the flow and removes its stored state.

Register `bus.Handle` for every Telebot event used by the steps.
For example, an inline-button flow also needs `bot.Handle(tele.OnCallback, bus.Handle)`.

## Context

Step callbacks work with two different contexts:

- `tele.Context` contains the current Telegram update and methods such as `Send`, `Edit`, `Text`, `Message`, and `Callback`.
- `teleflow.Context` contains the current flow state, typed data, metadata, and transition methods.

Use `teleflow.Context` to pass data between steps:

```go
if err := fc.SetString("company", "Acme"); err != nil {
	return fc.Stay(), err
}

company, ok := fc.GetString("company")
```

Typed methods are available for strings, booleans, integers, floating-point numbers, durations, and times.
`Delete` removes a value, while `Has` checks whether a key exists.

The same context controls the next transition:

```go
return fc.Stay(), nil
return fc.Next(), nil
return fc.Back(), nil
return fc.Go("company_details"), nil
```

`Current` identifies the step whose handler is running, while `Step` reads the active step from the working session state.
`Depth` reports the retained history depth at the start of the callback, and `CanBack` reports whether that history contained a previous step.
A `teleflow.Context` belongs to one callback.
Its data changes are persisted only when the callback returns a valid transition without an error and the updated state is written successfully.

The standard library `context.Context` controls cancellation and deadlines for storage operations and context-aware callbacks.
It is accepted by `Bus.Start`, `Bus.HandleCtx`, and `Bus.Cancel`, and passed to storage methods.
Only callbacks registered with `Flow.StepContext` receive it; callbacks registered with `Flow.Step` do not.
Use `Bus.HandleCtx(ctx)` instead of `Bus.Handle` when update handling must observe cancellation or deadlines:

```go
handler := bus.HandleCtx(ctx)

bot.Handle(tele.OnText, handler)
bot.Handle(tele.OnCallback, handler)
```

Use `Flow.StepContext` together with `Start(ctx, ...)` and `HandleCtx(ctx)` when step callbacks must observe cancellation:

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

## License

Teleflow is distributed under the [MIT License](LICENSE).
