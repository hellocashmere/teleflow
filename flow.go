package teleflow

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"sync"
	"time"

	tele "gopkg.in/telebot.v4"
)

const (
	// DefaultIdleTimeout is used when FlowConfig.IdleTimeout is zero.
	DefaultIdleTimeout = 24 * time.Hour

	// DefaultHistoryLimit is used when FlowConfig.HistoryLimit is zero.
	DefaultHistoryLimit = 64

	// MaxRuntimeStateSize limits serialized state loaded from or written to storage.
	MaxRuntimeStateSize = 1 << 20
)

var (
	ErrFlowNotBuilt     = errors.New("flow: definition is not built")
	ErrStepNameEmpty    = errors.New("flow: step name cannot be empty")
	ErrStepEventEmpty   = errors.New("flow: step event cannot be empty")
	ErrStepDuplicate    = errors.New("flow: duplicate step")
	ErrStepHandlerNil   = errors.New("flow: step handler cannot be nil")
	ErrRuntimeStateSize = errors.New("flow: runtime state is too large")
)

// FlowConfig is used to configure a flow.
type FlowConfig struct {
	// Name identifies the flow and must not be empty.
	Name string

	// Version identifies the flow definition and must be greater than zero.
	Version uint

	// IdleTimeout sets when inactive state expires and uses DefaultIdleTimeout when zero.
	IdleTimeout time.Duration

	// HistoryLimit sets the retained step count and uses DefaultHistoryLimit when zero.
	HistoryLimit int
}

// Flow describes a registered flow definition and its ordered steps.
type Flow struct {
	owner *bus

	name    string
	version uint

	idleTimeout  time.Duration
	historyLimit int

	steps     []*Step
	byName    map[string]*Step
	positions map[string]int

	mu    sync.RWMutex
	built bool
}

type runtimeState struct {
	FlowName     string                     `json:"flow_name"`
	FlowVersion  uint                       `json:"flow_version"`
	CurrentStep  string                     `json:"current_step"`
	History      []string                   `json:"history"`
	Data         map[string]json.RawMessage `json:"data"`
	StartedAt    time.Time                  `json:"started_at"`
	LastActivity time.Time                  `json:"last_activity"`
	LastUpdateID int                        `json:"last_update_id,omitempty"`
	PendingBegin bool                       `json:"pending_begin,omitempty"`
	Revision     uint64                     `json:"revision,omitempty"`
	LeaseUntil   time.Time                  `json:"lease_until,omitzero"`
}

func cloneRuntime(src *runtimeState) *runtimeState {
	dst := *src

	dst.History = append([]string(nil), src.History...)
	dst.Data = make(map[string]json.RawMessage, len(src.Data))
	for k, v := range src.Data {
		dst.Data[k] = append(json.RawMessage(nil), v...)
	}

	return &dst
}

func encodeRuntime(state *runtimeState) ([]byte, error) {
	value, err := json.Marshal(state)
	if err != nil {
		return nil, fmt.Errorf("flow: encode runtime state: %w", err)
	}

	if len(value) > MaxRuntimeStateSize {
		return nil, ErrRuntimeStateSize
	}

	return value, nil
}

func decodeRuntime(value []byte) (*runtimeState, error) {
	if len(value) == 0 {
		return nil, ErrInvalidRuntimeState
	}

	if len(value) > MaxRuntimeStateSize {
		return nil, ErrRuntimeStateSize
	}

	var state runtimeState
	if err := json.Unmarshal(value, &state); err != nil {
		return nil, fmt.Errorf("%w: %w", ErrInvalidRuntimeState, err)
	}

	invalidIdentity := state.FlowName == "" || state.FlowVersion == 0 || state.CurrentStep == ""
	invalidTime := state.StartedAt.IsZero() || state.LastActivity.IsZero() || state.LastActivity.Before(state.StartedAt)
	if invalidIdentity || invalidTime || len(state.History) == 0 {
		return nil, ErrInvalidRuntimeState
	}

	if state.Data == nil {
		state.Data = make(map[string]json.RawMessage)
	}

	for _, value := range state.Data {
		if !json.Valid(value) {
			return nil, ErrInvalidRuntimeState
		}
	}

	return &state, nil
}

// Name returns the flow definition name.
func (f *Flow) Name() string {
	return f.name
}

// Version returns the flow definition version.
func (f *Flow) Version() uint {
	return f.version
}

// Step adds a named step to the flow draft.
func (f *Flow) Step(
	name string,
	event string,
	begin BeginFunc,
	handle HandleFunc,
) {
	var beginContext BeginContextFunc
	if begin != nil {
		beginContext = func(_ context.Context, c tele.Context) error {
			return begin(c)
		}
	}

	var handleContext HandleContextFunc
	if handle != nil {
		handleContext = func(_ context.Context, c tele.Context, flowContext Context) (StepResult, error) {
			return handle(c, flowContext)
		}
	}

	f.addStep(name, event, beginContext, handleContext)
}

// StepContext adds a context-aware named step to the flow draft.
func (f *Flow) StepContext(
	name string,
	event string,
	begin BeginContextFunc,
	handle HandleContextFunc,
) {
	f.addStep(name, event, begin, handle)
}

func (f *Flow) addStep(
	name string,
	event string,
	begin BeginContextFunc,
	handle HandleContextFunc,
) {
	f.mu.Lock()
	defer f.mu.Unlock()

	if f.built {
		return
	}

	f.steps = append(f.steps, &Step{
		name:   name,
		on:     event,
		begin:  begin,
		handle: handle,
	})
}

// Build validates and registers the immutable flow definition.
func (f *Flow) Build() error {
	f.mu.Lock()
	defer f.mu.Unlock()

	if f.built {
		return nil
	}

	if f.name == "" {
		return ErrNameEmpty
	}

	if f.version == 0 {
		return ErrVersionInvalid
	}

	if f.idleTimeout < 0 {
		return ErrIdleTimeoutNegative
	}

	if f.historyLimit < 0 {
		return ErrHistoryLimitNegative
	}

	if len(f.steps) == 0 {
		return ErrNoStepsDefined
	}

	byName := make(map[string]*Step, len(f.steps))
	positions := make(map[string]int, len(f.steps))
	for i, step := range f.steps {
		if step.name == "" {
			return ErrStepNameEmpty
		}

		if step.on == "" {
			return fmt.Errorf("%w: step %q", ErrStepEventEmpty, step.name)
		}

		if step.handle == nil {
			return fmt.Errorf("%w: step %q", ErrStepHandlerNil, step.name)
		}

		if _, exists := byName[step.name]; exists {
			return fmt.Errorf("%w: %q", ErrStepDuplicate, step.name)
		}

		byName[step.name] = step
		positions[step.name] = i
	}

	if f.owner == nil {
		return ErrDefinitionNotRegistered
	}

	if f.idleTimeout == 0 {
		f.idleTimeout = DefaultIdleTimeout
	}

	if f.historyLimit == 0 {
		f.historyLimit = DefaultHistoryLimit
	}

	if err := f.owner.register(f); err != nil {
		return err
	}

	f.byName = byName
	f.positions = positions
	f.built = true

	return nil
}

func (f *Flow) ready() error {
	f.mu.RLock()
	defer f.mu.RUnlock()

	if !f.built {
		return ErrFlowNotBuilt
	}

	return nil
}

func (f *Flow) step(name string) *Step {
	f.mu.RLock()
	defer f.mu.RUnlock()

	return f.byName[name]
}

func (f *Flow) stepAt(i int) *Step {
	f.mu.RLock()
	defer f.mu.RUnlock()

	if i < 0 || i >= len(f.steps) {
		return nil
	}

	return f.steps[i]
}

func (f *Flow) next(name string) *Step {
	f.mu.RLock()
	defer f.mu.RUnlock()

	i, ok := f.positions[name]
	if !ok || i+1 >= len(f.steps) {
		return nil
	}

	return f.steps[i+1]
}

func (f *Flow) first() *Step {
	return f.stepAt(0)
}

func (f *Flow) validateRuntime(state *runtimeState) error {
	f.mu.RLock()
	defer f.mu.RUnlock()

	if state.FlowName != f.name || state.FlowVersion != f.version {
		return ErrInvalidRuntimeState
	}

	if len(state.History) == 0 || len(state.History) > f.historyLimit {
		return ErrInvalidRuntimeState
	}

	if state.History[len(state.History)-1] != state.CurrentStep {
		return ErrInvalidRuntimeState
	}

	for _, name := range state.History {
		if _, exists := f.byName[name]; !exists {
			return ErrInvalidRuntimeState
		}
	}

	return nil
}
