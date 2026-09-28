package teleflow

import (
	"context"
	"encoding/json"
	"errors"
	"strings"
	"testing"
	"time"

	"github.com/hellocashmere/teleflow/storage"
	tele "gopkg.in/telebot.v4"
)

func TestCloneRuntime(t *testing.T) {
	tests := []struct {
		name string
		test func(t *testing.T)
	}{
		{
			name: "clone_runtime",
			test: func(t *testing.T) {
				t.Parallel()

				source := &runtimeState{
					History: []string{"first", "second"},
					Data: map[string]json.RawMessage{
						"name": json.RawMessage(`"cashmere"`),
					},
				}

				cloned := cloneRuntime(source)
				cloned.History[0] = "changed"
				cloned.Data["name"][1] = 'X'
				cloned.Data["new"] = json.RawMessage(`true`)

				if got := source.History[0]; got != "first" {
					t.Fatalf("source history = %q, want %q", got, "first")
				}
				if got := string(source.Data["name"]); got != `"cashmere"` {
					t.Fatalf("source data = %q, want %q", got, `"cashmere"`)
				}
				if _, exists := source.Data["new"]; exists {
					t.Fatal("new key exists in source data")
				}
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, tt.test)
	}
}

func TestEncodeRuntime(t *testing.T) {
	t.Parallel()

	now := time.Date(2026, time.September, 27, 12, 0, 0, 0, time.UTC)
	tests := []struct {
		name       string
		state      *runtimeState
		wantErr    error
		wantAnyErr bool
	}{
		{
			name: "valid_state",
			state: &runtimeState{
				FlowName:     "signup",
				FlowVersion:  1,
				CurrentStep:  "name",
				History:      []string{"name"},
				Data:         map[string]json.RawMessage{},
				StartedAt:    now,
				LastActivity: now,
			},
		},
		{
			name: "invalid_raw_message",
			state: &runtimeState{
				Data: map[string]json.RawMessage{
					"value": json.RawMessage("{"),
				},
			},
			wantAnyErr: true,
		},
		{
			name: "state_too_large",
			state: &runtimeState{
				Data: map[string]json.RawMessage{
					"value": json.RawMessage(`"` + strings.Repeat("x", MaxRuntimeStateSize) + `"`),
				},
			},
			wantErr: ErrRuntimeStateSize,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			value, err := encodeRuntime(tt.state)
			if tt.wantAnyErr {
				if err == nil {
					t.Fatal("encodeRuntime() error = nil, want error")
				}
				return
			}
			if !errors.Is(err, tt.wantErr) {
				t.Fatalf("encodeRuntime() error = %v, want %v", err, tt.wantErr)
			}
			if tt.wantErr == nil && len(value) == 0 {
				t.Fatal("encodeRuntime() returned an empty value")
			}
		})
	}
}

func TestDecodeRuntime(t *testing.T) {
	t.Parallel()

	now := time.Date(2026, time.September, 27, 12, 0, 0, 0, time.UTC)
	validState := runtimeState{
		FlowName:     "signup",
		FlowVersion:  1,
		CurrentStep:  "name",
		History:      []string{"name"},
		Data:         map[string]json.RawMessage{},
		StartedAt:    now,
		LastActivity: now,
	}

	tests := []struct {
		name          string
		value         []byte
		wantErr       error
		wantDataReady bool
	}{
		{
			name:          "valid_state",
			value:         marshalRuntimeForTest(t, validState),
			wantDataReady: true,
		},
		{
			name: "nil_data_is_initialized",
			value: marshalRuntimeForTest(t, runtimeState{
				FlowName:     "signup",
				FlowVersion:  1,
				CurrentStep:  "name",
				History:      []string{"name"},
				StartedAt:    now,
				LastActivity: now,
			}),
			wantDataReady: true,
		},
		{
			name:    "empty_value",
			wantErr: ErrInvalidRuntimeState,
		},
		{
			name:    "malformed_json",
			value:   []byte("{"),
			wantErr: ErrInvalidRuntimeState,
		},
		{
			name:    "value_too_large",
			value:   make([]byte, MaxRuntimeStateSize+1),
			wantErr: ErrRuntimeStateSize,
		},
		{
			name: "missing_flow_name",
			value: marshalRuntimeForTest(t, runtimeState{
				FlowVersion:  1,
				CurrentStep:  "name",
				History:      []string{"name"},
				StartedAt:    now,
				LastActivity: now,
			}),
			wantErr: ErrInvalidRuntimeState,
		},
		{
			name: "missing_flow_version",
			value: marshalRuntimeForTest(t, runtimeState{
				FlowName:     "signup",
				CurrentStep:  "name",
				History:      []string{"name"},
				StartedAt:    now,
				LastActivity: now,
			}),
			wantErr: ErrInvalidRuntimeState,
		},
		{
			name: "missing_current_step",
			value: marshalRuntimeForTest(t, runtimeState{
				FlowName:     "signup",
				FlowVersion:  1,
				History:      []string{"name"},
				StartedAt:    now,
				LastActivity: now,
			}),
			wantErr: ErrInvalidRuntimeState,
		},
		{
			name: "empty_history",
			value: marshalRuntimeForTest(t, runtimeState{
				FlowName:     "signup",
				FlowVersion:  1,
				CurrentStep:  "name",
				StartedAt:    now,
				LastActivity: now,
			}),
			wantErr: ErrInvalidRuntimeState,
		},
		{
			name: "zero_started_at",
			value: marshalRuntimeForTest(t, runtimeState{
				FlowName:     "signup",
				FlowVersion:  1,
				CurrentStep:  "name",
				History:      []string{"name"},
				LastActivity: now,
			}),
			wantErr: ErrInvalidRuntimeState,
		},
		{
			name: "last_activity_before_start",
			value: marshalRuntimeForTest(t, runtimeState{
				FlowName:     "signup",
				FlowVersion:  1,
				CurrentStep:  "name",
				History:      []string{"name"},
				StartedAt:    now,
				LastActivity: now.Add(-time.Second),
			}),
			wantErr: ErrInvalidRuntimeState,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			state, err := decodeRuntime(tt.value)
			if !errors.Is(err, tt.wantErr) {
				t.Fatalf("decodeRuntime() error = %v, want %v", err, tt.wantErr)
			}
			if tt.wantErr != nil {
				return
			}
			if state == nil {
				t.Fatal("decodeRuntime() state = nil")
			}
			if tt.wantDataReady && state.Data == nil {
				t.Fatal("decodeRuntime() data = nil, want initialized map")
			}
		})
	}
}

func TestNameAndVersion(t *testing.T) {
	tests := []struct {
		name string
		test func(t *testing.T)
	}{
		{
			name: "name_and_version",
			test: func(t *testing.T) {
				t.Parallel()

				flow := NewBus(storage.NewMemory()).NewFlow(FlowConfig{
					Name:    "signup",
					Version: 2,
				})

				if got := flow.Name(); got != "signup" {
					t.Fatalf("Name() = %q, want %q", got, "signup")
				}
				if got := flow.Version(); got != 2 {
					t.Fatalf("Version() = %d, want 2", got)
				}
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, tt.test)
	}
}

func TestStep(t *testing.T) {
	tests := []struct {
		name string
		test func(t *testing.T)
	}{
		{
			name: "step",
			test: func(t *testing.T) {
				t.Parallel()

				flow := NewBus(storage.NewMemory()).NewFlow(FlowConfig{
					Name:    "signup",
					Version: 1,
				})
				beginCalled := false
				handleCalled := false

				flow.Step(
					"name",
					tele.OnText,
					func(c tele.Context) error {
						beginCalled = true
						return nil
					},
					func(c tele.Context, fc Context) (StepResult, error) {
						handleCalled = true
						return fc.Stay(), nil
					},
				)

				step := flow.first()
				ctx := &nativeContext{
					state: &runtimeState{},
				}
				if err := step.begin(context.Background(), nil); err != nil {
					t.Fatalf("begin() error = %v", err)
				}
				result, err := step.handle(context.Background(), nil, ctx)
				if err != nil {
					t.Fatalf("handle() error = %v", err)
				}
				if !beginCalled || !handleCalled {
					t.Fatalf("callbacks called = %t, %t, want true, true", beginCalled, handleCalled)
				}
				if result != stepStay {
					t.Fatalf("handle() result = %d, want %d", result, stepStay)
				}
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, tt.test)
	}
}

func TestStepContext(t *testing.T) {
	tests := []struct {
		name string
		test func(t *testing.T)
	}{
		{
			name: "step_context",
			test: func(t *testing.T) {
				t.Parallel()

				flow := NewBus(storage.NewMemory()).NewFlow(FlowConfig{
					Name:    "signup",
					Version: 1,
				})
				ctxKey := struct{}{}
				wantValue := "request"

				flow.StepContext(
					"name",
					tele.OnText,
					func(ctx context.Context, c tele.Context) error {
						if got := ctx.Value(ctxKey); got != wantValue {
							t.Fatalf("begin context value = %v, want %q", got, wantValue)
						}
						return nil
					},
					func(ctx context.Context, c tele.Context, fc Context) (StepResult, error) {
						if got := ctx.Value(ctxKey); got != wantValue {
							t.Fatalf("handle context value = %v, want %q", got, wantValue)
						}
						return fc.Next(), nil
					},
				)

				step := flow.first()
				callbackCtx := context.WithValue(context.Background(), ctxKey, wantValue)
				flowCtx := &nativeContext{
					state: &runtimeState{},
				}
				if err := step.begin(callbackCtx, nil); err != nil {
					t.Fatalf("begin() error = %v", err)
				}
				result, err := step.handle(callbackCtx, nil, flowCtx)
				if err != nil {
					t.Fatalf("handle() error = %v", err)
				}
				if result != stepNext {
					t.Fatalf("handle() result = %d, want %d", result, stepNext)
				}
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, tt.test)
	}
}

func TestBuildValidation(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name    string
		prepare func() *Flow
		wantErr error
	}{
		{
			name: "empty_name",
			prepare: func() *Flow {
				flow := validFlowDraft()
				flow.name = ""
				return flow
			},
			wantErr: ErrNameEmpty,
		},
		{
			name: "invalid_version",
			prepare: func() *Flow {
				flow := validFlowDraft()
				flow.version = 0
				return flow
			},
			wantErr: ErrVersionInvalid,
		},
		{
			name: "negative_idle_timeout",
			prepare: func() *Flow {
				flow := validFlowDraft()
				flow.idleTimeout = -time.Second
				return flow
			},
			wantErr: ErrIdleTimeoutNegative,
		},
		{
			name: "negative_history_limit",
			prepare: func() *Flow {
				flow := validFlowDraft()
				flow.historyLimit = -1
				return flow
			},
			wantErr: ErrHistoryLimitNegative,
		},
		{
			name: "no_steps",
			prepare: func() *Flow {
				flow := validFlowDraft()
				flow.steps = []*Step{}
				return flow
			},
			wantErr: ErrNoStepsDefined,
		},
		{
			name: "empty_step_name",
			prepare: func() *Flow {
				flow := validFlowDraft()
				flow.steps[0].name = ""
				return flow
			},
			wantErr: ErrStepNameEmpty,
		},
		{
			name: "empty_step_event",
			prepare: func() *Flow {
				flow := validFlowDraft()
				flow.steps[0].on = ""
				return flow
			},
			wantErr: ErrStepEventEmpty,
		},
		{
			name: "nil_step_handler",
			prepare: func() *Flow {
				flow := validFlowDraft()
				flow.steps[0].handle = nil
				return flow
			},
			wantErr: ErrStepHandlerNil,
		},
		{
			name: "duplicate_step",
			prepare: func() *Flow {
				flow := validFlowDraft()
				flow.StepContext("first", tele.OnText, nil, stayHandler)
				return flow
			},
			wantErr: ErrStepDuplicate,
		},
		{
			name: "missing_owner",
			prepare: func() *Flow {
				flow := validFlowDraft()
				flow.owner = nil
				return flow
			},
			wantErr: ErrDefinitionNotRegistered,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			flow := tt.prepare()
			if err := flow.Build(); !errors.Is(err, tt.wantErr) {
				t.Fatalf("Build() error = %v, want %v", err, tt.wantErr)
			}
		})
	}
}

func TestBuildAppliesDefaults(t *testing.T) {
	tests := []struct {
		name string
		test func(t *testing.T)
	}{
		{
			name: "build_applies_defaults",
			test: func(t *testing.T) {
				t.Parallel()

				flow := validFlowDraft()
				if err := flow.Build(); err != nil {
					t.Fatalf("Build() error = %v", err)
				}

				if flow.idleTimeout != DefaultIdleTimeout {
					t.Fatalf("idle timeout = %v, want %v", flow.idleTimeout, DefaultIdleTimeout)
				}
				if flow.historyLimit != DefaultHistoryLimit {
					t.Fatalf("history limit = %d, want %d", flow.historyLimit, DefaultHistoryLimit)
				}
				if !flow.built {
					t.Fatal("built = false, want true")
				}
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, tt.test)
	}
}

func TestBuildPreservesExplicitValues(t *testing.T) {
	tests := []struct {
		name string
		test func(t *testing.T)
	}{
		{
			name: "build_preserves_explicit_values",
			test: func(t *testing.T) {
				t.Parallel()

				flow := validFlowDraft()
				flow.idleTimeout = 5 * time.Minute
				flow.historyLimit = 3

				if err := flow.Build(); err != nil {
					t.Fatalf("Build() error = %v", err)
				}
				if flow.idleTimeout != 5*time.Minute {
					t.Fatalf("idle timeout = %v, want %v", flow.idleTimeout, 5*time.Minute)
				}
				if flow.historyLimit != 3 {
					t.Fatalf("history limit = %d, want 3", flow.historyLimit)
				}
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, tt.test)
	}
}

func TestBuildIsIdempotent(t *testing.T) {
	tests := []struct {
		name string
		test func(t *testing.T)
	}{
		{
			name: "build_is_idempotent",
			test: func(t *testing.T) {
				t.Parallel()

				flow := validFlowDraft()
				if err := flow.Build(); err != nil {
					t.Fatalf("first Build() error = %v", err)
				}
				if err := flow.Build(); err != nil {
					t.Fatalf("second Build() error = %v", err)
				}
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, tt.test)
	}
}

func TestBuildRejectsDuplicateDefinition(t *testing.T) {
	tests := []struct {
		name string
		test func(t *testing.T)
	}{
		{
			name: "build_rejects_duplicate_definition",
			test: func(t *testing.T) {
				t.Parallel()

				bus := NewBus(storage.NewMemory())
				first := bus.NewFlow(FlowConfig{
					Name:    "signup",
					Version: 1,
				})
				first.StepContext("first", tele.OnText, nil, stayHandler)
				second := bus.NewFlow(FlowConfig{
					Name:    "signup",
					Version: 1,
				})
				second.StepContext("first", tele.OnText, nil, stayHandler)

				if err := first.Build(); err != nil {
					t.Fatalf("first Build() error = %v", err)
				}
				if err := second.Build(); !errors.Is(err, ErrDefinitionExists) {
					t.Fatalf("second Build() error = %v, want %v", err, ErrDefinitionExists)
				}
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, tt.test)
	}
}

func TestStepAfterBuildIsIgnored(t *testing.T) {
	tests := []struct {
		name string
		test func(t *testing.T)
	}{
		{
			name: "step_after_build_is_ignored",
			test: func(t *testing.T) {
				t.Parallel()

				flow := validFlowDraft()
				if err := flow.Build(); err != nil {
					t.Fatalf("Build() error = %v", err)
				}

				flow.StepContext("second", tele.OnText, nil, stayHandler)
				if got := len(flow.steps); got != 1 {
					t.Fatalf("step count = %d, want 1", got)
				}
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, tt.test)
	}
}

func TestNavigation(t *testing.T) {
	tests := []struct {
		name string
		test func(t *testing.T)
	}{
		{
			name: "navigation",
			test: func(t *testing.T) {
				t.Parallel()

				flow := validFlowDraft()
				flow.StepContext("second", tele.OnText, nil, stayHandler)
				if err := flow.Build(); err != nil {
					t.Fatalf("Build() error = %v", err)
				}

				if got := flow.first(); got == nil || got.name != "first" {
					t.Fatalf("first() = %#v, want first step", got)
				}
				if got := flow.step("second"); got == nil || got.name != "second" {
					t.Fatalf("step() = %#v, want second step", got)
				}
				if got := flow.stepAt(-1); got != nil {
					t.Fatalf("stepAt(-1) = %#v, want nil", got)
				}
				if got := flow.stepAt(2); got != nil {
					t.Fatalf("stepAt(2) = %#v, want nil", got)
				}
				if got := flow.next("first"); got == nil || got.name != "second" {
					t.Fatalf("next(first) = %#v, want second step", got)
				}
				if got := flow.next("second"); got != nil {
					t.Fatalf("next(second) = %#v, want nil", got)
				}
				if got := flow.next("missing"); got != nil {
					t.Fatalf("next(missing) = %#v, want nil", got)
				}
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, tt.test)
	}
}

func TestValidateRuntime(t *testing.T) {
	t.Parallel()

	flow := validFlowDraft()
	flow.StepContext("second", tele.OnText, nil, stayHandler)
	if err := flow.Build(); err != nil {
		t.Fatalf("Build() error = %v", err)
	}

	now := time.Date(2026, time.September, 27, 12, 0, 0, 0, time.UTC)
	validState := &runtimeState{
		FlowName:     "signup",
		FlowVersion:  1,
		CurrentStep:  "second",
		History:      []string{"first", "second"},
		Data:         map[string]json.RawMessage{},
		StartedAt:    now,
		LastActivity: now,
	}

	tests := []struct {
		name    string
		mutate  func(*runtimeState)
		wantErr error
	}{
		{
			name: "valid_state",
			mutate: func(state *runtimeState) {
			},
		},
		{
			name: "different_flow",
			mutate: func(state *runtimeState) {
				state.FlowName = "other"
			},
			wantErr: ErrInvalidRuntimeState,
		},
		{
			name: "different_version",
			mutate: func(state *runtimeState) {
				state.FlowVersion = 2
			},
			wantErr: ErrInvalidRuntimeState,
		},
		{
			name: "empty_history",
			mutate: func(state *runtimeState) {
				state.History = []string{}
			},
			wantErr: ErrInvalidRuntimeState,
		},
		{
			name: "history_too_long",
			mutate: func(state *runtimeState) {
				state.History = make([]string, DefaultHistoryLimit+1)
				for i := range state.History {
					state.History[i] = "first"
				}
				state.CurrentStep = "first"
			},
			wantErr: ErrInvalidRuntimeState,
		},
		{
			name: "current_step_is_not_history_tail",
			mutate: func(state *runtimeState) {
				state.CurrentStep = "first"
			},
			wantErr: ErrInvalidRuntimeState,
		},
		{
			name: "unknown_history_step",
			mutate: func(state *runtimeState) {
				state.History = []string{"missing"}
				state.CurrentStep = "missing"
			},
			wantErr: ErrInvalidRuntimeState,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			state := cloneRuntime(validState)
			tt.mutate(state)
			if err := flow.validateRuntime(state); !errors.Is(err, tt.wantErr) {
				t.Fatalf("validateRuntime() error = %v, want %v", err, tt.wantErr)
			}
		})
	}
}

func validFlowDraft() *Flow {
	flow := NewBus(storage.NewMemory()).NewFlow(FlowConfig{
		Name:    "signup",
		Version: 1,
	})
	flow.StepContext("first", tele.OnText, nil, stayHandler)

	return flow
}

func stayHandler(context.Context, tele.Context, Context) (StepResult, error) {
	return stepStay, nil
}

func marshalRuntimeForTest(t *testing.T, state runtimeState) []byte {
	t.Helper()

	value, err := json.Marshal(state)
	if err != nil {
		t.Fatalf("json.Marshal() error = %v", err)
	}

	return value
}
