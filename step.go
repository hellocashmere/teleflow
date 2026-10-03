package teleflow

import (
	"context"

	tele "gopkg.in/telebot.v4"
)

// StepResult describes the transition requested by a step handler.
//
// Use the transition methods on Context to obtain a value.
type StepResult uint8

const (
	stepUnknown StepResult = iota
	stepStay
	stepNext
	stepBack
	stepGo
)

// BeginFunc runs when a session enters a step.
//
// Teleflow may call it again when an earlier attempt or the following state write did not complete.
type BeginFunc func(tele.Context) error

// HandleFunc processes a matching event and returns the next transition.
//
// Session data and the transition are persisted only after the callback returns a valid transition without an error and the state write succeeds.
type HandleFunc func(tele.Context, Context) (StepResult, error)

// BeginContextFunc is the context-aware form of BeginFunc.
type BeginContextFunc func(context.Context, tele.Context) error

// HandleContextFunc is the context-aware form of HandleFunc.
type HandleContextFunc func(context.Context, tele.Context, Context) (StepResult, error)

// Step describes a named flow step and the Telebot event it handles.
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
