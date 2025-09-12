# teleflow
```bash
go get -u github.com/hellocashmere/teleflow
```

* [Overview](#overview)
* [Getting Started](#getting-started)
* [License](#license)

# Overview
Teleflow is a package which is designed to solve the problem of dialogs using [telebot](https://github.com/tucnak/telebot).
It provides a simple and efficient way to receive and process both input text and is capable of handling button clicks, location sending, contacts, etc.

# Getting Started
A simple example of getting username and password from a user:
```go
package main

import (
	"context"
	"github.com/hellocashmere/teleflow"
	"github.com/hellocashmere/teleflow/storage"
	tele "gopkg.in/telebot.v4"
	"log"
	"os"
	"time"
)

func main() {
	pref := tele.Settings{
		Token:  os.Getenv("TOKEN"),
		Poller: &tele.LongPoller{Timeout: 10 * time.Second},
	}

	bot, err := tele.NewBot(pref)
	if err != nil {
		log.Fatal(err)
		return
	}
	
	states := storage.NewMemory()
	
	bus := teleflow.NewBus(states)
	
	bot.Handle(tele.OnText, bus.Handle)
	bot.Handle(tele.OnCallback, bus.Handle)

	bot.Handle("/signup", func(c tele.Context) error {
		ctx := context.Background()

		f := bus.Flow("signup", 1*time.Minute)

		var username string

		f.OnText(func(state *teleflow.State) error {
			return state.Context.EditOrSend("Enter username:")
		}).Assign(func(state *teleflow.State) error {
			username = state.Context.Message().Text
			return nil
		})

		f.OnText(func(state *teleflow.State) error {
			return state.Context.EditOrSend("Enter password:")
		}).Ok(func(state *teleflow.State) error {
			log.Println("username:", username)
			log.Println("password:", state.Context.Message().Text)

			return nil
		})

		return bus.Start(ctx, c, f)
	})

	log.Println("starting telegram bot...")
	bot.Start()
}
```

Simple, innit? Teleflow's primary goal was to make it easy and understandable to use in our bots, which is what we strive for to this day (if you have an idea, create an issue/PR)

# License
Teleflow is distributed under MIT.