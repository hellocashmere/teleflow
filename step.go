package teleflow

import (
	"context"

	tele "gopkg.in/telebot.v4"
)

// StepResult describes the transition requested by a step handler.
type StepResult uint8

const (
	stepUnknown StepResult = iota
	stepStay
	stepNext
	stepBack
	stepGo
)

// BeginFunc is called when a flow enters a step.
type BeginFunc func(tele.Context) error

// HandleFunc processes an event received by the current step.
type HandleFunc func(tele.Context, Context) (StepResult, error)

// BeginContextFunc is a context-aware step entry handler.
type BeginContextFunc func(context.Context, tele.Context) error

// HandleContextFunc is a context-aware step event handler.
type HandleContextFunc func(context.Context, tele.Context, Context) (StepResult, error)

type Step struct {
	name   string
	on     string
	begin  BeginContextFunc
	handle HandleContextFunc
}

// Name returns the stable identifier of the step.
func (s *Step) Name() string {
	return s.name
}

// Event returns the Telebot event type handled by the step.
func (s *Step) Event() string {
	return s.on
}
