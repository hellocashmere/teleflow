package teleflow

import (
	"context"
	"errors"
	"fmt"
	"math"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/hellocashmere/teleflow/storage"
	tele "gopkg.in/telebot.v4"
)

func TestNewBus(t *testing.T) {
	tests := []struct {
		name string
		test func(t *testing.T)
	}{
		{
			name: "new_bus",
			test: func(t *testing.T) {
				t.Parallel()

				store := storage.NewMemory()
				busInstance := NewBus(store)
				if busInstance == nil {
					t.Fatal("NewBus() = nil")
				}

				native, ok := busInstance.(*bus)
				if !ok {
					t.Fatalf("NewBus() type = %T, want *bus", busInstance)
				}
				if native.store != store {
					t.Fatal("NewBus() did not retain storage")
				}
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, tt.test)
	}
}

func TestNewFlow(t *testing.T) {
	tests := []struct {
		name string
		test func(t *testing.T)
	}{
		{
			name: "new_flow",
			test: func(t *testing.T) {
				t.Parallel()

				bus := NewBus(storage.NewMemory())
				flow := bus.NewFlow(FlowConfig{
					Name:         "signup",
					Version:      2,
					IdleTimeout:  5 * time.Minute,
					HistoryLimit: 3,
				})

				if flow.owner == nil {
					t.Fatal("flow owner = nil")
				}
				if flow.name != "signup" {
					t.Fatalf("flow name = %q, want %q", flow.name, "signup")
				}
				if flow.version != 2 {
					t.Fatalf("flow version = %d, want 2", flow.version)
				}
				if flow.idleTimeout != 5*time.Minute {
					t.Fatalf("flow idle timeout = %v, want %v", flow.idleTimeout, 5*time.Minute)
				}
				if flow.historyLimit != 3 {
					t.Fatalf("flow history limit = %d, want 3", flow.historyLimit)
				}
				if flow.steps == nil {
					t.Fatal("flow steps = nil, want initialized slice")
				}
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, tt.test)
	}
}

func TestStart(t *testing.T) {
	tests := []struct {
		name string
		test func(t *testing.T)
	}{
		{
			name: "start",
			test: func(t *testing.T) {
				t.Parallel()

				store := storage.NewMemory()
				bus := NewBus(store)
				ctx := textContextForTest(1, 10, 20, 0, "start")
				beginCalls := 0
				flow := bus.NewFlow(FlowConfig{
					Name:    "signup",
					Version: 1,
				})
				flow.StepContext(
					"name",
					tele.OnText,
					func(context.Context, tele.Context) error {
						beginCalls++
						return nil
					},
					stayHandler,
				)
				if err := flow.Build(); err != nil {
					t.Fatalf("Build() error = %v", err)
				}

				if err := bus.Start(t.Context(), ctx, flow); err != nil {
					t.Fatalf("Start() error = %v", err)
				}
				if beginCalls != 1 {
					t.Fatalf("begin calls = %d, want 1", beginCalls)
				}

				state := loadRuntimeForTest(t, store, ctx)
				if state.CurrentStep != "name" {
					t.Fatalf("current step = %q, want %q", state.CurrentStep, "name")
				}
				if state.PendingBegin {
					t.Fatal("pending begin = true, want false")
				}
				if !state.LeaseUntil.IsZero() {
					t.Fatalf("lease until = %v, want zero", state.LeaseUntil)
				}
				if state.Revision != 1 {
					t.Fatalf("revision = %d, want 1", state.Revision)
				}

				if err := bus.Start(t.Context(), ctx, flow); err != nil {
					t.Fatalf("second Start() error = %v", err)
				}
				if beginCalls != 1 {
					t.Fatalf("begin calls after second Start() = %d, want 1", beginCalls)
				}
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, tt.test)
	}
}

func TestStartValidation(t *testing.T) {
	t.Parallel()

	validContext := textContextForTest(1, 10, 20, 0, "start")
	tests := []struct {
		name    string
		start   func(context.Context, tele.Context) error
		ctx     tele.Context
		wantErr error
	}{
		{
			name: "nil_storage",
			start: func(ctx context.Context, c tele.Context) error {
				return NewBus(nil).Start(ctx, c, nil)
			},
			ctx:     validContext,
			wantErr: ErrStorageNil,
		},
		{
			name: "nil_flow",
			start: func(ctx context.Context, c tele.Context) error {
				return NewBus(storage.NewMemory()).Start(ctx, c, nil)
			},
			ctx:     validContext,
			wantErr: ErrNilDefinition,
		},
		{
			name: "flow_not_built",
			start: func(ctx context.Context, c tele.Context) error {
				bus := NewBus(storage.NewMemory())
				flow := bus.NewFlow(FlowConfig{
					Name:    "signup",
					Version: 1,
				})
				return bus.Start(ctx, c, flow)
			},
			ctx:     validContext,
			wantErr: ErrFlowNotBuilt,
		},
		{
			name: "definition_not_registered_on_bus",
			start: func(ctx context.Context, c tele.Context) error {
				firstBus := NewBus(storage.NewMemory())
				flow := firstBus.NewFlow(FlowConfig{
					Name:    "signup",
					Version: 1,
				})
				flow.StepContext("name", tele.OnText, nil, stayHandler)
				if err := flow.Build(); err != nil {
					return err
				}

				return NewBus(storage.NewMemory()).Start(ctx, c, flow)
			},
			ctx:     validContext,
			wantErr: ErrDefinitionNotRegistered,
		},
		{
			name: "context_without_sender",
			start: func(ctx context.Context, c tele.Context) error {
				bus := NewBus(storage.NewMemory())
				flow := bus.NewFlow(FlowConfig{
					Name:    "signup",
					Version: 1,
				})
				flow.StepContext("name", tele.OnText, nil, stayHandler)
				if err := flow.Build(); err != nil {
					return err
				}

				return bus.Start(ctx, c, flow)
			},
			ctx:     tele.NewContext(nil, tele.Update{}),
			wantErr: ErrContextNoSender,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			if err := tt.start(t.Context(), tt.ctx); !errors.Is(err, tt.wantErr) {
				t.Fatalf("Start() error = %v, want %v", err, tt.wantErr)
			}
		})
	}
}

func TestStartRetriesPendingBegin(t *testing.T) {
	tests := []struct {
		name string
		test func(t *testing.T)
	}{
		{
			name: "start_retries_pending_begin",
			test: func(t *testing.T) {
				t.Parallel()

				callbackErr := errors.New("begin failed")
				store := storage.NewMemory()
				bus := NewBus(store)
				ctx := textContextForTest(1, 10, 20, 0, "start")
				beginCalls := 0
				flow := bus.NewFlow(FlowConfig{
					Name:    "signup",
					Version: 1,
				})
				flow.StepContext(
					"name",
					tele.OnText,
					func(context.Context, tele.Context) error {
						beginCalls++
						if beginCalls == 1 {
							return callbackErr
						}
						return nil
					},
					stayHandler,
				)
				if err := flow.Build(); err != nil {
					t.Fatalf("Build() error = %v", err)
				}

				if err := bus.Start(t.Context(), ctx, flow); !errors.Is(err, callbackErr) {
					t.Fatalf("first Start() error = %v, want %v", err, callbackErr)
				}
				failedState := loadRuntimeForTest(t, store, ctx)
				if !failedState.PendingBegin {
					t.Fatal("pending begin after error = false, want true")
				}
				if !failedState.LeaseUntil.IsZero() {
					t.Fatalf("lease after error = %v, want zero", failedState.LeaseUntil)
				}

				if err := bus.Start(t.Context(), ctx, flow); err != nil {
					t.Fatalf("second Start() error = %v", err)
				}
				if beginCalls != 2 {
					t.Fatalf("begin calls = %d, want 2", beginCalls)
				}
				readyState := loadRuntimeForTest(t, store, ctx)
				if readyState.PendingBegin {
					t.Fatal("pending begin after retry = true, want false")
				}
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, tt.test)
	}
}

func TestStartCanceledBeginReleasesLease(t *testing.T) {
	tests := []struct {
		name string
		test func(t *testing.T)
	}{
		{
			name: "start_canceled_begin_releases_lease",
			test: func(t *testing.T) {
				t.Parallel()

				store := storage.NewMemory()
				bus := NewBus(store)
				teleCtx := textContextForTest(1, 10, 20, 0, "start")
				ctx, cancel := context.WithCancel(t.Context())
				flow := bus.NewFlow(FlowConfig{
					Name:    "signup",
					Version: 1,
				})
				flow.StepContext(
					"name",
					tele.OnText,
					func(callbackCtx context.Context, c tele.Context) error {
						cancel()
						<-callbackCtx.Done()
						return callbackCtx.Err()
					},
					stayHandler,
				)
				if err := flow.Build(); err != nil {
					t.Fatalf("Build() error = %v", err)
				}

				if err := bus.Start(ctx, teleCtx, flow); !errors.Is(err, context.Canceled) {
					t.Fatalf("Start() error = %v, want %v", err, context.Canceled)
				}
				state := loadRuntimeForTest(t, store, teleCtx)
				if !state.LeaseUntil.IsZero() {
					t.Fatalf("lease after cancellation = %v, want zero", state.LeaseUntil)
				}
				if !state.PendingBegin {
					t.Fatal("pending begin after cancellation = false, want true")
				}
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, tt.test)
	}
}

func TestHandleStay(t *testing.T) {
	tests := []struct {
		name string
		test func(t *testing.T)
	}{
		{
			name: "handle_stay",
			test: func(t *testing.T) {
				t.Parallel()

				store := storage.NewMemory()
				bus := NewBus(store)
				startCtx := textContextForTest(1, 10, 20, 0, "start")
				flow := bus.NewFlow(FlowConfig{
					Name:    "signup",
					Version: 1,
				})
				flow.StepContext(
					"name",
					tele.OnText,
					nil,
					func(ctx context.Context, c tele.Context, fc Context) (StepResult, error) {
						if err := fc.SetString("name", "cashmere"); err != nil {
							return fc.Stay(), err
						}
						return fc.Stay(), nil
					},
				)
				if err := flow.Build(); err != nil {
					t.Fatalf("Build() error = %v", err)
				}
				if err := bus.Start(t.Context(), startCtx, flow); err != nil {
					t.Fatalf("Start() error = %v", err)
				}

				updateCtx := textContextForTest(2, 10, 20, 0, "cashmere")
				if err := bus.Handle(updateCtx); err != nil {
					t.Fatalf("Handle() error = %v", err)
				}

				state := loadRuntimeForTest(t, store, updateCtx)
				if state.CurrentStep != "name" {
					t.Fatalf("current step = %q, want %q", state.CurrentStep, "name")
				}
				if state.LastUpdateID != 2 {
					t.Fatalf("last update ID = %d, want 2", state.LastUpdateID)
				}
				if got := string(state.Data["name"]); got != `"cashmere"` {
					t.Fatalf("stored name = %s, want %s", got, `"cashmere"`)
				}
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, tt.test)
	}
}

func TestHandleNext(t *testing.T) {
	tests := []struct {
		name string
		test func(t *testing.T)
	}{
		{
			name: "handle_next",
			test: func(t *testing.T) {
				t.Parallel()

				store := storage.NewMemory()
				bus := NewBus(store)
				startCtx := textContextForTest(1, 10, 20, 0, "start")
				secondBeginCalls := 0
				flow := bus.NewFlow(FlowConfig{
					Name:    "signup",
					Version: 1,
				})
				flow.StepContext(
					"name",
					tele.OnText,
					nil,
					func(ctx context.Context, c tele.Context, fc Context) (StepResult, error) {
						return fc.Next(), nil
					},
				)
				flow.StepContext(
					"confirm",
					tele.OnText,
					func(context.Context, tele.Context) error {
						secondBeginCalls++
						return nil
					},
					stayHandler,
				)
				if err := flow.Build(); err != nil {
					t.Fatalf("Build() error = %v", err)
				}
				if err := bus.Start(t.Context(), startCtx, flow); err != nil {
					t.Fatalf("Start() error = %v", err)
				}

				updateCtx := textContextForTest(2, 10, 20, 0, "cashmere")
				if err := bus.Handle(updateCtx); err != nil {
					t.Fatalf("Handle() error = %v", err)
				}

				state := loadRuntimeForTest(t, store, updateCtx)
				if state.CurrentStep != "confirm" {
					t.Fatalf("current step = %q, want %q", state.CurrentStep, "confirm")
				}
				if state.PendingBegin {
					t.Fatal("pending begin = true, want false")
				}
				if secondBeginCalls != 1 {
					t.Fatalf("second begin calls = %d, want 1", secondBeginCalls)
				}
				if len(state.History) != 2 {
					t.Fatalf("history length = %d, want 2", len(state.History))
				}
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, tt.test)
	}
}

func TestHandleNextCompletesFlow(t *testing.T) {
	tests := []struct {
		name string
		test func(t *testing.T)
	}{
		{
			name: "handle_next_completes_flow",
			test: func(t *testing.T) {
				t.Parallel()

				store := storage.NewMemory()
				bus := NewBus(store)
				ctx := textContextForTest(1, 10, 20, 0, "start")
				flow := bus.NewFlow(FlowConfig{
					Name:    "signup",
					Version: 1,
				})
				flow.StepContext(
					"name",
					tele.OnText,
					nil,
					func(ctx context.Context, c tele.Context, fc Context) (StepResult, error) {
						return fc.Next(), nil
					},
				)
				if err := flow.Build(); err != nil {
					t.Fatalf("Build() error = %v", err)
				}
				if err := bus.Start(t.Context(), ctx, flow); err != nil {
					t.Fatalf("Start() error = %v", err)
				}

				if err := bus.Handle(textContextForTest(2, 10, 20, 0, "done")); err != nil {
					t.Fatalf("Handle() error = %v", err)
				}
				assertRuntimeMissingForTest(t, store, ctx)
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, tt.test)
	}
}

func TestHandleGo(t *testing.T) {
	tests := []struct {
		name string
		test func(t *testing.T)
	}{
		{
			name: "handle_go",
			test: func(t *testing.T) {
				t.Parallel()

				store := storage.NewMemory()
				bus := NewBus(store)
				startCtx := textContextForTest(1, 10, 20, 0, "start")
				secondBeginCalls := 0
				thirdBeginCalls := 0
				flow := bus.NewFlow(FlowConfig{
					Name:    "signup",
					Version: 1,
				})
				flow.StepContext(
					"name",
					tele.OnText,
					nil,
					func(ctx context.Context, c tele.Context, fc Context) (StepResult, error) {
						return fc.Go("company_details"), nil
					},
				)
				flow.StepContext(
					"personal_details",
					tele.OnText,
					func(context.Context, tele.Context) error {
						secondBeginCalls++
						return nil
					},
					stayHandler,
				)
				flow.StepContext(
					"company_details",
					tele.OnText,
					func(context.Context, tele.Context) error {
						thirdBeginCalls++
						return nil
					},
					stayHandler,
				)
				if err := flow.Build(); err != nil {
					t.Fatalf("Build() error = %v", err)
				}
				if err := bus.Start(t.Context(), startCtx, flow); err != nil {
					t.Fatalf("Start() error = %v", err)
				}

				updateCtx := textContextForTest(2, 10, 20, 0, "company")
				if err := bus.Handle(updateCtx); err != nil {
					t.Fatalf("Handle() error = %v", err)
				}

				state := loadRuntimeForTest(t, store, updateCtx)
				if state.CurrentStep != "company_details" {
					t.Fatalf("current step = %q, want %q", state.CurrentStep, "company_details")
				}
				if state.PendingBegin {
					t.Fatal("pending begin = true, want false")
				}
				if len(state.History) != 2 {
					t.Fatalf("history length = %d, want 2", len(state.History))
				}
				if state.History[1] != "company_details" {
					t.Fatalf("history target = %q, want %q", state.History[1], "company_details")
				}
				if secondBeginCalls != 0 {
					t.Fatalf("skipped step begin calls = %d, want 0", secondBeginCalls)
				}
				if thirdBeginCalls != 1 {
					t.Fatalf("target step begin calls = %d, want 1", thirdBeginCalls)
				}
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, tt.test)
	}
}

func TestHandleGoRejectsUnknownStep(t *testing.T) {
	tests := []struct {
		name string
		test func(t *testing.T)
	}{
		{
			name: "handle_go_rejects_unknown_step",
			test: func(t *testing.T) {
				t.Parallel()

				store := storage.NewMemory()
				bus := NewBus(store)
				startCtx := textContextForTest(1, 10, 20, 0, "start")
				flow := bus.NewFlow(FlowConfig{
					Name:    "signup",
					Version: 1,
				})
				flow.StepContext(
					"name",
					tele.OnText,
					nil,
					func(ctx context.Context, c tele.Context, fc Context) (StepResult, error) {
						if err := fc.SetString("name", "cashmere"); err != nil {
							return fc.Stay(), err
						}

						return fc.Go("missing"), nil
					},
				)
				if err := flow.Build(); err != nil {
					t.Fatalf("Build() error = %v", err)
				}
				if err := bus.Start(t.Context(), startCtx, flow); err != nil {
					t.Fatalf("Start() error = %v", err)
				}

				updateCtx := textContextForTest(2, 10, 20, 0, "cashmere")
				err := bus.Handle(updateCtx)
				if !errors.Is(err, ErrTargetStepNotFound) {
					t.Fatalf("Handle() error = %v, want %v", err, ErrTargetStepNotFound)
				}

				state := loadRuntimeForTest(t, store, updateCtx)
				if state.CurrentStep != "name" {
					t.Fatalf("current step = %q, want %q", state.CurrentStep, "name")
				}
				if !state.LeaseUntil.IsZero() {
					t.Fatalf("lease after rejected target = %v, want zero", state.LeaseUntil)
				}
				if state.LastUpdateID != 0 {
					t.Fatalf("last update ID = %d, want 0", state.LastUpdateID)
				}
				if len(state.Data) != 0 {
					t.Fatalf("state data = %v, want empty", state.Data)
				}
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, tt.test)
	}
}

func TestHandleBack(t *testing.T) {
	tests := []struct {
		name string
		test func(t *testing.T)
	}{
		{
			name: "handle_back",
			test: func(t *testing.T) {
				t.Parallel()

				store := storage.NewMemory()
				bus := NewBus(store)
				ctx := textContextForTest(1, 10, 20, 0, "start")
				firstBeginCalls := 0
				flow := bus.NewFlow(FlowConfig{
					Name:    "signup",
					Version: 1,
				})
				flow.StepContext(
					"first",
					tele.OnText,
					func(context.Context, tele.Context) error {
						firstBeginCalls++
						return nil
					},
					func(ctx context.Context, c tele.Context, fc Context) (StepResult, error) {
						return fc.Next(), nil
					},
				)
				flow.StepContext(
					"second",
					tele.OnText,
					nil,
					func(ctx context.Context, c tele.Context, fc Context) (StepResult, error) {
						return fc.Back(), nil
					},
				)
				if err := flow.Build(); err != nil {
					t.Fatalf("Build() error = %v", err)
				}
				if err := bus.Start(t.Context(), ctx, flow); err != nil {
					t.Fatalf("Start() error = %v", err)
				}
				if err := bus.Handle(textContextForTest(2, 10, 20, 0, "next")); err != nil {
					t.Fatalf("next Handle() error = %v", err)
				}
				if err := bus.Handle(textContextForTest(3, 10, 20, 0, "back")); err != nil {
					t.Fatalf("back Handle() error = %v", err)
				}

				state := loadRuntimeForTest(t, store, ctx)
				if state.CurrentStep != "first" {
					t.Fatalf("current step = %q, want %q", state.CurrentStep, "first")
				}
				if len(state.History) != 1 {
					t.Fatalf("history length = %d, want 1", len(state.History))
				}
				if firstBeginCalls != 2 {
					t.Fatalf("first begin calls = %d, want 2", firstBeginCalls)
				}
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, tt.test)
	}
}

func TestHandleBackAtFirstStep(t *testing.T) {
	tests := []struct {
		name string
		test func(t *testing.T)
	}{
		{
			name: "handle_back_at_first_step",
			test: func(t *testing.T) {
				t.Parallel()

				store := storage.NewMemory()
				bus := NewBus(store)
				ctx := textContextForTest(1, 10, 20, 0, "start")
				flow := bus.NewFlow(FlowConfig{
					Name:    "signup",
					Version: 1,
				})
				flow.StepContext(
					"first",
					tele.OnText,
					nil,
					func(ctx context.Context, c tele.Context, fc Context) (StepResult, error) {
						return fc.Back(), nil
					},
				)
				if err := flow.Build(); err != nil {
					t.Fatalf("Build() error = %v", err)
				}
				if err := bus.Start(t.Context(), ctx, flow); err != nil {
					t.Fatalf("Start() error = %v", err)
				}

				if err := bus.Handle(textContextForTest(2, 10, 20, 0, "back")); err != nil {
					t.Fatalf("Handle() error = %v", err)
				}
				state := loadRuntimeForTest(t, store, ctx)
				if state.CurrentStep != "first" || len(state.History) != 1 {
					t.Fatalf("state = step %q, history %v, want first and one entry", state.CurrentStep, state.History)
				}
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, tt.test)
	}
}

func TestHandleIgnoresDifferentEvent(t *testing.T) {
	tests := []struct {
		name string
		test func(t *testing.T)
	}{
		{
			name: "handle_ignores_different_event",
			test: func(t *testing.T) {
				t.Parallel()

				store := storage.NewMemory()
				bus := NewBus(store)
				ctx := textContextForTest(1, 10, 20, 0, "start")
				handleCalls := 0
				flow := bus.NewFlow(FlowConfig{
					Name:    "survey",
					Version: 1,
				})
				flow.StepContext(
					"answer",
					tele.OnCallback,
					nil,
					func(ctx context.Context, c tele.Context, fc Context) (StepResult, error) {
						handleCalls++
						return fc.Stay(), nil
					},
				)
				if err := flow.Build(); err != nil {
					t.Fatalf("Build() error = %v", err)
				}
				if err := bus.Start(t.Context(), ctx, flow); err != nil {
					t.Fatalf("Start() error = %v", err)
				}

				if err := bus.Handle(textContextForTest(2, 10, 20, 0, "text")); err != nil {
					t.Fatalf("Handle() error = %v", err)
				}
				if handleCalls != 0 {
					t.Fatalf("handle calls = %d, want 0", handleCalls)
				}
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, tt.test)
	}
}

func TestHandleIgnoresDuplicateUpdate(t *testing.T) {
	tests := []struct {
		name string
		test func(t *testing.T)
	}{
		{
			name: "handle_ignores_duplicate_update",
			test: func(t *testing.T) {
				t.Parallel()

				store := storage.NewMemory()
				bus := NewBus(store)
				ctx := textContextForTest(1, 10, 20, 0, "start")
				handleCalls := 0
				flow := bus.NewFlow(FlowConfig{
					Name:    "signup",
					Version: 1,
				})
				flow.StepContext(
					"name",
					tele.OnText,
					nil,
					func(ctx context.Context, c tele.Context, fc Context) (StepResult, error) {
						handleCalls++
						return fc.Stay(), nil
					},
				)
				if err := flow.Build(); err != nil {
					t.Fatalf("Build() error = %v", err)
				}
				if err := bus.Start(t.Context(), ctx, flow); err != nil {
					t.Fatalf("Start() error = %v", err)
				}

				updateCtx := textContextForTest(2, 10, 20, 0, "name")
				if err := bus.Handle(updateCtx); err != nil {
					t.Fatalf("first Handle() error = %v", err)
				}
				if err := bus.Handle(updateCtx); err != nil {
					t.Fatalf("second Handle() error = %v", err)
				}
				if handleCalls != 1 {
					t.Fatalf("handle calls = %d, want 1", handleCalls)
				}
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, tt.test)
	}
}

func TestHandleErrorReleasesLease(t *testing.T) {
	tests := []struct {
		name string
		test func(t *testing.T)
	}{
		{
			name: "handle_error_releases_lease",
			test: func(t *testing.T) {
				t.Parallel()

				callbackErr := errors.New("handler failed")
				store := storage.NewMemory()
				bus := NewBus(store)
				ctx := textContextForTest(1, 10, 20, 0, "start")
				flow := bus.NewFlow(FlowConfig{
					Name:    "signup",
					Version: 1,
				})
				flow.StepContext(
					"name",
					tele.OnText,
					nil,
					func(ctx context.Context, c tele.Context, fc Context) (StepResult, error) {
						if err := fc.SetString("name", "unsaved"); err != nil {
							return fc.Stay(), err
						}
						return fc.Stay(), callbackErr
					},
				)
				if err := flow.Build(); err != nil {
					t.Fatalf("Build() error = %v", err)
				}
				if err := bus.Start(t.Context(), ctx, flow); err != nil {
					t.Fatalf("Start() error = %v", err)
				}

				if err := bus.Handle(textContextForTest(2, 10, 20, 0, "name")); !errors.Is(err, callbackErr) {
					t.Fatalf("Handle() error = %v, want %v", err, callbackErr)
				}
				state := loadRuntimeForTest(t, store, ctx)
				if !state.LeaseUntil.IsZero() {
					t.Fatalf("lease until = %v, want zero", state.LeaseUntil)
				}
				if _, exists := state.Data["name"]; exists {
					t.Fatal("callback data was persisted after error")
				}
				if state.LastUpdateID != 0 {
					t.Fatalf("last update ID = %d, want 0", state.LastUpdateID)
				}
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, tt.test)
	}
}

func TestHandleInvalidResultReleasesLease(t *testing.T) {
	tests := []struct {
		name string
		test func(t *testing.T)
	}{
		{
			name: "handle_invalid_result_releases_lease",
			test: func(t *testing.T) {
				t.Parallel()

				store := storage.NewMemory()
				bus := NewBus(store)
				ctx := textContextForTest(1, 10, 20, 0, "start")
				flow := bus.NewFlow(FlowConfig{
					Name:    "signup",
					Version: 1,
				})
				flow.StepContext(
					"name",
					tele.OnText,
					nil,
					func(context.Context, tele.Context, Context) (StepResult, error) {
						return StepResult(255), nil
					},
				)
				if err := flow.Build(); err != nil {
					t.Fatalf("Build() error = %v", err)
				}
				if err := bus.Start(t.Context(), ctx, flow); err != nil {
					t.Fatalf("Start() error = %v", err)
				}

				if err := bus.Handle(textContextForTest(2, 10, 20, 0, "name")); !errors.Is(err, ErrInvalidStepResult) {
					t.Fatalf("Handle() error = %v, want %v", err, ErrInvalidStepResult)
				}
				state := loadRuntimeForTest(t, store, ctx)
				if !state.LeaseUntil.IsZero() {
					t.Fatalf("lease until = %v, want zero", state.LeaseUntil)
				}
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, tt.test)
	}
}

func TestHandleExpiredState(t *testing.T) {
	tests := []struct {
		name string
		test func(t *testing.T)
	}{
		{
			name: "handle_expired_state",
			test: func(t *testing.T) {
				t.Parallel()

				store := storage.NewMemory()
				bus := NewBus(store).(*bus)
				now := time.Now()
				bus.now = func() time.Time {
					return now
				}
				ctx := textContextForTest(1, 10, 20, 0, "start")
				flow := bus.NewFlow(FlowConfig{
					Name:        "signup",
					Version:     1,
					IdleTimeout: time.Minute,
				})
				flow.StepContext("name", tele.OnText, nil, stayHandler)
				if err := flow.Build(); err != nil {
					t.Fatalf("Build() error = %v", err)
				}
				if err := bus.Start(t.Context(), ctx, flow); err != nil {
					t.Fatalf("Start() error = %v", err)
				}

				now = now.Add(time.Minute)
				if err := bus.Handle(textContextForTest(2, 10, 20, 0, "name")); !errors.Is(err, ErrExpired) {
					t.Fatalf("Handle() error = %v, want %v", err, ErrExpired)
				}
				assertRuntimeMissingForTest(t, store, ctx)
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, tt.test)
	}
}

func TestHandleLiveLeaseReturnsStateBusy(t *testing.T) {
	tests := []struct {
		name string
		test func(t *testing.T)
	}{
		{
			name: "handle_live_lease_returns_state_busy",
			test: func(t *testing.T) {
				t.Parallel()

				store := storage.NewMemory()
				bus := NewBus(store).(*bus)
				now := time.Now()
				bus.now = func() time.Time {
					return now
				}
				ctx := textContextForTest(1, 10, 20, 0, "start")
				flow := bus.NewFlow(FlowConfig{
					Name:    "signup",
					Version: 1,
				})
				flow.StepContext("name", tele.OnText, nil, stayHandler)
				if err := flow.Build(); err != nil {
					t.Fatalf("Build() error = %v", err)
				}
				if err := bus.Start(t.Context(), ctx, flow); err != nil {
					t.Fatalf("Start() error = %v", err)
				}

				state := loadRuntimeForTest(t, store, ctx)
				state.LeaseUntil = now.Add(time.Minute)
				storeRuntimeForTest(t, store, ctx, state)

				if err := bus.Handle(textContextForTest(2, 10, 20, 0, "name")); !errors.Is(err, ErrStateBusy) {
					t.Fatalf("Handle() error = %v, want %v", err, ErrStateBusy)
				}
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, tt.test)
	}
}

func TestHandleRejectsRevisionOverflow(t *testing.T) {
	tests := []struct {
		name string
		test func(t *testing.T)
	}{
		{
			name: "handle_rejects_revision_overflow",
			test: func(t *testing.T) {
				t.Parallel()

				store := storage.NewMemory()
				bus := NewBus(store)
				ctx := textContextForTest(1, 10, 20, 0, "start")
				flow := bus.NewFlow(FlowConfig{
					Name:    "signup",
					Version: 1,
				})
				flow.StepContext("name", tele.OnText, nil, stayHandler)
				if err := flow.Build(); err != nil {
					t.Fatalf("Build() error = %v", err)
				}
				if err := bus.Start(t.Context(), ctx, flow); err != nil {
					t.Fatalf("Start() error = %v", err)
				}

				state := loadRuntimeForTest(t, store, ctx)
				state.Revision = math.MaxUint64
				storeRuntimeForTest(t, store, ctx, state)

				if err := bus.Handle(textContextForTest(2, 10, 20, 0, "name")); !errors.Is(err, ErrInvalidRuntimeState) {
					t.Fatalf("Handle() error = %v, want %v", err, ErrInvalidRuntimeState)
				}
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, tt.test)
	}
}

func TestHandleTwoBusesProcessUpdateOnce(t *testing.T) {
	tests := []struct {
		name string
		test func(t *testing.T)
	}{
		{
			name: "handle_two_buses_process_update_once",
			test: func(t *testing.T) {
				t.Parallel()

				store := storage.NewMemory()
				firstBus := NewBus(store)
				secondBus := NewBus(store)
				started := make(chan struct{})
				release := make(chan struct{})
				var handleCalls atomic.Int32

				newFlow := func(bus Bus) *Flow {
					flow := bus.NewFlow(FlowConfig{
						Name:    "signup",
						Version: 1,
					})
					flow.StepContext(
						"name",
						tele.OnText,
						nil,
						func(ctx context.Context, c tele.Context, fc Context) (StepResult, error) {
							if handleCalls.Add(1) == 1 {
								close(started)
								<-release
							}
							return fc.Stay(), nil
						},
					)
					if err := flow.Build(); err != nil {
						t.Fatalf("Build() error = %v", err)
					}
					return flow
				}

				firstFlow := newFlow(firstBus)
				newFlow(secondBus)
				startCtx := textContextForTest(1, 10, 20, 0, "start")
				if err := firstBus.Start(t.Context(), startCtx, firstFlow); err != nil {
					t.Fatalf("Start() error = %v", err)
				}

				updateCtx := textContextForTest(2, 10, 20, 0, "name")
				firstResult := make(chan error, 1)
				go func() {
					firstResult <- firstBus.Handle(updateCtx)
				}()
				<-started

				secondErr := secondBus.Handle(updateCtx)
				close(release)
				firstErr := <-firstResult

				if !errors.Is(secondErr, ErrStateBusy) {
					t.Fatalf("second Handle() error = %v, want %v", secondErr, ErrStateBusy)
				}
				if firstErr != nil {
					t.Fatalf("first Handle() error = %v", firstErr)
				}
				if err := secondBus.Handle(updateCtx); err != nil {
					t.Fatalf("duplicate Handle() error = %v", err)
				}
				if got := handleCalls.Load(); got != 1 {
					t.Fatalf("handle calls = %d, want 1", got)
				}
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, tt.test)
	}
}

func TestHandleIsolatesSessions(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name         string
		firstUser    int64
		firstChat    int64
		firstThread  int
		secondUser   int64
		secondChat   int64
		secondThread int
	}{
		{
			name:       "different_users_same_chat",
			firstUser:  10,
			firstChat:  20,
			secondUser: 11,
			secondChat: 20,
		},
		{
			name:       "same_user_different_chats",
			firstUser:  10,
			firstChat:  20,
			secondUser: 10,
			secondChat: 21,
		},
		{
			name:         "same_user_same_chat_different_threads",
			firstUser:    10,
			firstChat:    20,
			firstThread:  30,
			secondUser:   10,
			secondChat:   20,
			secondThread: 31,
		},
		{
			name:       "numeric_ids_do_not_collide",
			firstUser:  1,
			firstChat:  23,
			secondUser: 12,
			secondChat: 3,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			store := storage.NewMemory()
			bus := NewBus(store)
			flow := newIsolationFlowForTest(t, bus)

			firstStart := textContextForTest(
				1,
				tt.firstUser,
				tt.firstChat,
				tt.firstThread,
				"start",
			)
			secondStart := textContextForTest(
				1,
				tt.secondUser,
				tt.secondChat,
				tt.secondThread,
				"start",
			)

			if err := bus.Start(t.Context(), firstStart, flow); err != nil {
				t.Fatalf("first Start() error = %v", err)
			}
			if err := bus.Start(t.Context(), secondStart, flow); err != nil {
				t.Fatalf("second Start() error = %v", err)
			}

			firstUpdate := textContextForTest(
				2,
				tt.firstUser,
				tt.firstChat,
				tt.firstThread,
				"first",
			)
			secondUpdate := textContextForTest(
				2,
				tt.secondUser,
				tt.secondChat,
				tt.secondThread,
				"second",
			)

			if err := bus.Handle(firstUpdate); err != nil {
				t.Fatalf("first Handle() error = %v", err)
			}
			if err := bus.Handle(secondUpdate); err != nil {
				t.Fatalf("second Handle() error = %v", err)
			}

			firstState := loadRuntimeForTest(t, store, firstUpdate)
			secondState := loadRuntimeForTest(t, store, secondUpdate)

			if got := string(firstState.Data["answer"]); got != `"first"` {
				t.Fatalf("first answer = %s, want %s", got, `"first"`)
			}
			if got := string(secondState.Data["answer"]); got != `"second"` {
				t.Fatalf("second answer = %s, want %s", got, `"second"`)
			}

			if err := bus.Cancel(t.Context(), firstUpdate); err != nil {
				t.Fatalf("first Cancel() error = %v", err)
			}

			assertRuntimeMissingForTest(t, store, firstUpdate)

			secondState = loadRuntimeForTest(t, store, secondUpdate)
			if got := string(secondState.Data["answer"]); got != `"second"` {
				t.Fatalf("second answer after first cancel = %s, want %s", got, `"second"`)
			}
		})
	}
}

func TestHandleIsolatesConcurrentUsers(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name     string
		users    int
		replicas int
	}{
		{
			name:     "shared_memory_across_four_replicas",
			users:    128,
			replicas: 4,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			store := storage.NewMemory()
			buses := make([]Bus, tt.replicas)
			flows := make([]*Flow, tt.replicas)

			for i := range tt.replicas {
				buses[i] = NewBus(store)
				flows[i] = newIsolationFlowForTest(t, buses[i])
			}

			start := make(chan struct{})
			errCh := make(chan error, tt.users)
			var wg sync.WaitGroup

			wg.Add(tt.users)
			for i := range tt.users {
				go func() {
					defer wg.Done()
					<-start

					userID := int64(i + 1)
					busIndex := i % tt.replicas
					startCtx := textContextForTest(1, userID, 100, 0, "start")
					updateCtx := textContextForTest(2, userID, 100, 0, fmt.Sprintf("user_%d", userID))

					if err := buses[busIndex].Start(t.Context(), startCtx, flows[busIndex]); err != nil {
						errCh <- fmt.Errorf("user %d start: %w", userID, err)
						return
					}
					if err := buses[busIndex].Handle(updateCtx); err != nil {
						errCh <- fmt.Errorf("user %d handle: %w", userID, err)
					}
				}()
			}

			close(start)
			wg.Wait()
			close(errCh)

			hasErrors := false
			for err := range errCh {
				hasErrors = true
				t.Error(err)
			}
			if hasErrors {
				return
			}

			for i := range tt.users {
				userID := int64(i + 1)
				busIndex := i % tt.replicas
				ctx := textContextForTest(2, userID, 100, 0, fmt.Sprintf("user_%d", userID))
				state := loadRuntimeForTest(t, store, ctx)
				want := fmt.Sprintf(`"user_%d"`, userID)

				if got := string(state.Data["answer"]); got != want {
					t.Fatalf("user %d answer = %s, want %s", userID, got, want)
				}

				if err := buses[busIndex].Cancel(t.Context(), ctx); err != nil {
					t.Fatalf("user %d Cancel() error = %v", userID, err)
				}
			}
		})
	}
}

func TestCancel(t *testing.T) {
	tests := []struct {
		name string
		test func(t *testing.T)
	}{
		{
			name: "cancel",
			test: func(t *testing.T) {
				t.Parallel()

				store := storage.NewMemory()
				bus := NewBus(store)
				ctx := textContextForTest(1, 10, 20, 0, "start")
				flow := bus.NewFlow(FlowConfig{
					Name:    "signup",
					Version: 1,
				})
				flow.StepContext("name", tele.OnText, nil, stayHandler)
				if err := flow.Build(); err != nil {
					t.Fatalf("Build() error = %v", err)
				}
				if err := bus.Start(t.Context(), ctx, flow); err != nil {
					t.Fatalf("Start() error = %v", err)
				}

				if err := bus.Cancel(t.Context(), ctx); err != nil {
					t.Fatalf("Cancel() error = %v", err)
				}
				assertRuntimeMissingForTest(t, store, ctx)
				if err := bus.Cancel(t.Context(), ctx); err != nil {
					t.Fatalf("second Cancel() error = %v", err)
				}
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, tt.test)
	}
}

func TestCancelValidation(t *testing.T) {
	tests := []struct {
		name string
		test func(t *testing.T)
	}{
		{
			name: "cancel_validation",
			test: func(t *testing.T) {
				t.Parallel()

				if err := NewBus(nil).Cancel(t.Context(), textContextForTest(1, 10, 20, 0, "cancel")); !errors.Is(err, ErrStorageNil) {
					t.Fatalf("Cancel() nil storage error = %v, want %v", err, ErrStorageNil)
				}
				if err := NewBus(storage.NewMemory()).Cancel(t.Context(), tele.NewContext(nil, tele.Update{})); !errors.Is(err, ErrContextNoSender) {
					t.Fatalf("Cancel() missing sender error = %v, want %v", err, ErrContextNoSender)
				}
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, tt.test)
	}
}

func TestGetEventType(t *testing.T) {
	t.Parallel()

	bus := &bus{}
	tests := []struct {
		name string
		ctx  tele.Context
		want string
	}{
		{
			name: "nil_context",
		},
		{
			name: "text",
			ctx:  textContextForTest(1, 10, 20, 0, "text"),
			want: tele.OnText,
		},
		{
			name: "photo",
			ctx: tele.NewContext(nil, tele.Update{
				Message: &tele.Message{
					Sender: &tele.User{ID: 10},
					Chat:   &tele.Chat{ID: 20},
					Photo:  &tele.Photo{},
				},
			}),
			want: tele.OnPhoto,
		},
		{
			name: "callback_has_priority",
			ctx: tele.NewContext(nil, tele.Update{
				Callback: &tele.Callback{
					Sender: &tele.User{ID: 10},
					Message: &tele.Message{
						Chat: &tele.Chat{ID: 20},
						Text: "text",
					},
				},
			}),
			want: tele.OnCallback,
		},
		{
			name: "empty_update",
			ctx:  tele.NewContext(nil, tele.Update{}),
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			if got := bus.getEventType(tt.ctx); got != tt.want {
				t.Fatalf("getEventType() = %q, want %q", got, tt.want)
			}
		})
	}
}

func TestLockFor(t *testing.T) {
	tests := []struct {
		name string
		test func(t *testing.T)
	}{
		{
			name: "lock_for",
			test: func(t *testing.T) {
				t.Parallel()

				bus := &bus{
					userLocks: map[string]*keyedLock{},
				}

				unlock := bus.lockFor("user")
				if got := len(bus.userLocks); got != 1 {
					t.Fatalf("lock count = %d, want 1", got)
				}
				unlock()
				if got := len(bus.userLocks); got != 0 {
					t.Fatalf("lock count after unlock = %d, want 0", got)
				}
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, tt.test)
	}
}

func TestExpired(t *testing.T) {
	t.Parallel()

	now := time.Date(2026, time.September, 27, 12, 0, 0, 0, time.UTC)
	bus := &bus{
		now: func() time.Time {
			return now
		},
	}
	flow := &Flow{idleTimeout: time.Minute}
	tests := []struct {
		name  string
		state *runtimeState
		want  bool
	}{
		{
			name: "zero_last_activity",
			state: &runtimeState{
				LastActivity: time.Time{},
			},
		},
		{
			name: "before_timeout",
			state: &runtimeState{
				LastActivity: now.Add(-time.Minute + time.Nanosecond),
			},
		},
		{
			name: "at_timeout",
			state: &runtimeState{
				LastActivity: now.Add(-time.Minute),
			},
			want: true,
		},
		{
			name: "active_lease_prevents_expiration",
			state: &runtimeState{
				LastActivity: now.Add(-2 * time.Minute),
				LeaseUntil:   now.Add(time.Minute),
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			if got := bus.expired(tt.state, flow); got != tt.want {
				t.Fatalf("expired() = %t, want %t", got, tt.want)
			}
		})
	}
}

func TestSessionKey(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name    string
		ctx     tele.Context
		want    string
		wantErr error
	}{
		{
			name:    "nil_context",
			wantErr: ErrContextNoSender,
		},
		{
			name:    "missing_sender",
			ctx:     tele.NewContext(nil, tele.Update{}),
			wantErr: ErrContextNoSender,
		},
		{
			name: "sender_and_chat",
			ctx:  textContextForTest(1, 10, 20, 0, "text"),
			want: "chat:20:user:10",
		},
		{
			name: "sender_chat_and_thread",
			ctx:  textContextForTest(1, 10, 20, 30, "text"),
			want: "chat:20:user:10:thread:30",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			got, err := sessionKey(tt.ctx)
			if !errors.Is(err, tt.wantErr) {
				t.Fatalf("sessionKey() error = %v, want %v", err, tt.wantErr)
			}
			if got != tt.want {
				t.Fatalf("sessionKey() = %q, want %q", got, tt.want)
			}
		})
	}
}

func TestAppendBounded(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name  string
		value []string
		id    string
		limit int
		want  []string
	}{
		{
			name:  "under_limit",
			value: []string{"first"},
			id:    "second",
			limit: 3,
			want:  []string{"first", "second"},
		},
		{
			name:  "trims_oldest_entry",
			value: []string{"first", "second"},
			id:    "third",
			limit: 2,
			want:  []string{"second", "third"},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			got := appendBounded(tt.value, tt.id, tt.limit)
			if fmt.Sprint(got) != fmt.Sprint(tt.want) {
				t.Fatalf("appendBounded() = %v, want %v", got, tt.want)
			}
		})
	}
}

func TestLeaseContext(t *testing.T) {
	tests := []struct {
		name string
		test func(t *testing.T)
	}{
		{
			name: "lease_context",
			test: func(t *testing.T) {
				t.Parallel()

				parent, parentCancel := context.WithCancel(t.Context())
				ctx, cancel := leaseContext(parent, time.Time{})
				parentCancel()
				defer cancel()

				if !errors.Is(ctx.Err(), context.Canceled) {
					t.Fatalf("leaseContext() error = %v, want %v", ctx.Err(), context.Canceled)
				}

				deadline := time.Now().Add(time.Minute)
				deadlineCtx, deadlineCancel := leaseContext(t.Context(), deadline)
				defer deadlineCancel()
				got, ok := deadlineCtx.Deadline()
				if !ok || !got.Equal(deadline) {
					t.Fatalf("leaseContext() deadline = %v, %t, want %v, true", got, ok, deadline)
				}
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, tt.test)
	}
}

func TestDefKey(t *testing.T) {
	tests := []struct {
		name string
		test func(t *testing.T)
	}{
		{
			name: "def_key",
			test: func(t *testing.T) {
				t.Parallel()

				if got := defKey("signup", 2); got != "signup\x002" {
					t.Fatalf("defKey() = %q, want %q", got, "signup\x002")
				}
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, tt.test)
	}
}

func BenchmarkSessionLifecycle(b *testing.B) {
	tests := []struct {
		name     string
		replicas int
	}{
		{
			name:     "one_replica",
			replicas: 1,
		},
		{
			name:     "four_replicas",
			replicas: 4,
		},
	}

	for _, tt := range tests {
		b.Run(tt.name, func(b *testing.B) {
			store := storage.NewMemory()
			buses := make([]Bus, tt.replicas)
			flows := make([]*Flow, tt.replicas)

			for i := range tt.replicas {
				buses[i] = NewBus(store)
				flows[i] = newIsolationFlowForTest(b, buses[i])
			}

			var sequence atomic.Int64

			b.ReportAllocs()
			b.ResetTimer()
			b.RunParallel(func(pb *testing.PB) {
				for pb.Next() {
					userID := sequence.Add(1)
					busIndex := int(userID % int64(tt.replicas))
					startCtx := textContextForTest(1, userID, 100, 0, "start")
					updateCtx := textContextForTest(2, userID, 100, 0, "answer")

					if err := buses[busIndex].Start(context.Background(), startCtx, flows[busIndex]); err != nil {
						b.Errorf("Start() error = %v", err)
						continue
					}
					if err := buses[busIndex].Handle(updateCtx); err != nil {
						b.Errorf("Handle() error = %v", err)
						continue
					}
					if err := buses[busIndex].Cancel(context.Background(), updateCtx); err != nil {
						b.Errorf("Cancel() error = %v", err)
					}
				}
			})
		})
	}
}

func newIsolationFlowForTest(tb testing.TB, bus Bus) *Flow {
	tb.Helper()

	flow := bus.NewFlow(FlowConfig{
		Name:    "isolation",
		Version: 1,
	})
	flow.StepContext(
		"answer",
		tele.OnText,
		nil,
		func(ctx context.Context, c tele.Context, fc Context) (StepResult, error) {
			if err := fc.SetString("answer", c.Text()); err != nil {
				return fc.Stay(), err
			}

			return fc.Stay(), nil
		},
	)

	if err := flow.Build(); err != nil {
		tb.Fatalf("Build() error = %v", err)
	}

	return flow
}

func textContextForTest(updateID int, userID, chatID int64, threadID int, text string) tele.Context {
	return tele.NewContext(nil, tele.Update{
		ID: updateID,
		Message: &tele.Message{
			ID:       updateID,
			ThreadID: threadID,
			Sender:   &tele.User{ID: userID},
			Chat:     &tele.Chat{ID: chatID},
			Text:     text,
		},
	})
}

func loadRuntimeForTest(t *testing.T, store storage.Storage, ctx tele.Context) *runtimeState {
	t.Helper()

	key, err := sessionKey(ctx)
	if err != nil {
		t.Fatalf("sessionKey() error = %v", err)
	}
	value, err := store.Get(t.Context(), key)
	if err != nil {
		t.Fatalf("Storage.Get() error = %v", err)
	}
	state, err := decodeRuntime(value)
	if err != nil {
		t.Fatalf("decodeRuntime() error = %v", err)
	}

	return state
}

func storeRuntimeForTest(t *testing.T, store storage.Storage, ctx tele.Context, state *runtimeState) {
	t.Helper()

	key, err := sessionKey(ctx)
	if err != nil {
		t.Fatalf("sessionKey() error = %v", err)
	}
	value, err := encodeRuntime(state)
	if err != nil {
		t.Fatalf("encodeRuntime() error = %v", err)
	}
	if err := store.Set(t.Context(), key, value, DefaultIdleTimeout); err != nil {
		t.Fatalf("Storage.Set() error = %v", err)
	}
}

func assertRuntimeMissingForTest(t *testing.T, store storage.Storage, ctx tele.Context) {
	t.Helper()

	key, err := sessionKey(ctx)
	if err != nil {
		t.Fatalf("sessionKey() error = %v", err)
	}
	if _, err := store.Get(t.Context(), key); !errors.Is(err, storage.ErrKeyNotFound) {
		t.Fatalf("Storage.Get() error = %v, want %v", err, storage.ErrKeyNotFound)
	}
}
