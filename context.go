package teleflow

import (
	"encoding/json"
	"errors"
	"fmt"
	"math"
	"strconv"
	"time"
)

var (
	ErrDataKeyEmpty = errors.New("flow: data key cannot be empty")
	ErrInvalidValue = errors.New("flow: invalid value")
)

// Context exposes session data and transition operations during one step handler call.
//
// Data changes are persisted only after the handler returns a valid transition without an error and the state write succeeds.
// Setters and Delete return ErrDataKeyEmpty when given an empty key.
type Context interface {
	// SetString stores a string under the key.
	SetString(string, string) error

	// SetBool stores a boolean under the key.
	SetBool(string, bool) error

	// SetInt stores an int value normalized to int64.
	SetInt(string, int) error

	// SetInt32 stores an int32 value normalized to int64.
	SetInt32(string, int32) error

	// SetInt64 stores an int64 under the key.
	SetInt64(string, int64) error

	// SetFloat32 stores a finite float32 value normalized to float64.
	SetFloat32(string, float32) error

	// SetFloat64 stores a finite float64 under the key.
	SetFloat64(string, float64) error

	// SetDuration stores a time.Duration as an int64 nanosecond count.
	SetDuration(string, time.Duration) error

	// SetTime stores a time.Time in RFC3339Nano format.
	SetTime(string, time.Time) error

	// Delete removes the value stored under the key.
	// It returns ErrDataKeyEmpty for an empty key and succeeds when the key is absent.
	Delete(string) error

	// Has reports whether the key exists in the current flow data.
	Has(string) bool

	// GetString returns the stored string and reports whether the key exists with that type.
	GetString(string) (string, bool)

	// GetBool returns the stored boolean and reports whether the key exists with that type.
	GetBool(string) (bool, bool)

	// GetInt returns the stored integer and reports whether it exists and fits in an int.
	GetInt(string) (int, bool)

	// GetInt32 returns the stored integer and reports whether it exists and fits in an int32.
	GetInt32(string) (int32, bool)

	// GetInt64 returns the stored int64 and reports whether the key exists with that type.
	GetInt64(string) (int64, bool)

	// GetFloat32 returns the stored number and reports whether it exists and converts without overflow or underflow.
	GetFloat32(string) (float32, bool)

	// GetFloat64 returns the stored float64 and reports whether the key exists with that type.
	GetFloat64(string) (float64, bool)

	// GetDuration returns the stored duration and reports whether the key exists with that type.
	GetDuration(string) (time.Duration, bool)

	// GetTime returns the stored time and reports whether the key contains a valid RFC3339Nano value.
	GetTime(string) (time.Time, bool)

	// Current returns the step whose handler is running.
	Current() string

	// Depth returns the number of retained history entries at the start of the handler call.
	Depth() int

	// CanBack reports whether a previous history entry existed at the start of the handler call.
	CanBack() bool

	// Step returns the active step recorded in the working session state.
	Step() string

	// Next requests the next defined step.
	// On the last step, successful handling completes the flow and removes the session state.
	Next() StepResult

	// Back requests the previous retained history entry.
	// At the first retained entry, the flow stays on the current step.
	Back() StepResult

	// Go requests the named step.
	// Handling returns ErrTargetStepNotFound when the flow has no step with that name.
	Go(string) StepResult

	// Stay requests no step change.
	// Successful handling persists any data changes.
	Stay() StepResult
}

// nativeContext implements Context.
type nativeContext struct {
	state   *runtimeState
	current string
	depth   int
	canBack bool
	target  string
}

func (c *nativeContext) set(k string, value any) error {
	if k == "" {
		return ErrDataKeyEmpty
	}
	encoded, err := json.Marshal(value)
	if err != nil {
		return fmt.Errorf("%w: %w", ErrInvalidValue, err)
	}
	if c.state.Data == nil {
		c.state.Data = make(map[string]json.RawMessage)
	}
	c.state.Data[k] = encoded
	return nil
}

func (c *nativeContext) SetString(k, value string) error {
	return c.set(k, value)
}

func (c *nativeContext) SetBool(k string, value bool) error {
	return c.set(k, value)
}

func (c *nativeContext) SetInt(k string, value int) error {
	return c.SetInt64(k, int64(value))
}

func (c *nativeContext) SetInt32(k string, value int32) error {
	return c.SetInt64(k, int64(value))
}

func (c *nativeContext) SetInt64(k string, value int64) error {
	return c.set(k, value)
}

func (c *nativeContext) SetFloat32(k string, value float32) error {
	return c.SetFloat64(k, float64(value))
}

func (c *nativeContext) SetFloat64(k string, value float64) error {
	if math.IsNaN(value) || math.IsInf(value, 0) {
		return ErrInvalidValue
	}
	return c.set(k, value)
}

func (c *nativeContext) SetDuration(k string, value time.Duration) error {
	return c.SetInt64(k, int64(value))
}

func (c *nativeContext) SetTime(k string, value time.Time) error {
	return c.SetString(k, value.Format(time.RFC3339Nano))
}

func (c *nativeContext) Delete(k string) error {
	if k == "" {
		return ErrDataKeyEmpty
	}
	delete(c.state.Data, k)
	return nil
}

func (c *nativeContext) Has(k string) bool {
	_, ok := c.state.Data[k]
	return ok
}

func (c *nativeContext) GetString(k string) (string, bool) {
	var value string
	ok := c.decode(k, &value)
	return value, ok
}

func (c *nativeContext) GetBool(k string) (bool, bool) {
	var value bool
	ok := c.decode(k, &value)
	return value, ok
}

func (c *nativeContext) GetInt(k string) (int, bool) {
	value, ok := c.GetInt64(k)
	if !ok {
		return 0, false
	}
	if strconv.IntSize == 32 && (value < math.MinInt32 || value > math.MaxInt32) {
		return 0, false
	}
	return int(value), true
}

func (c *nativeContext) GetInt32(k string) (int32, bool) {
	value, ok := c.GetInt64(k)
	if !ok || value < math.MinInt32 || value > math.MaxInt32 {
		return 0, false
	}
	return int32(value), true
}

func (c *nativeContext) GetInt64(k string) (int64, bool) {
	var value int64
	ok := c.decode(k, &value)
	return value, ok
}

func (c *nativeContext) GetFloat32(k string) (float32, bool) {
	value, ok := c.GetFloat64(k)
	if !ok || value < -math.MaxFloat32 || value > math.MaxFloat32 {
		return 0, false
	}
	converted := float32(value)
	if value != 0 && converted == 0 {
		return 0, false
	}
	return converted, true
}

func (c *nativeContext) GetFloat64(k string) (float64, bool) {
	var value float64
	ok := c.decode(k, &value)
	return value, ok
}

func (c *nativeContext) decode(k string, dst any) bool {
	value, ok := c.state.Data[k]
	if !ok {
		return false
	}
	return json.Unmarshal(value, dst) == nil
}

func (c *nativeContext) GetDuration(k string) (time.Duration, bool) {
	value, ok := c.GetInt64(k)
	return time.Duration(value), ok
}

// GetTime returns the RFC3339Nano time stored under k.
func (c *nativeContext) GetTime(k string) (time.Time, bool) {
	value, ok := c.GetString(k)
	if !ok {
		return time.Time{}, false
	}

	parsed, err := time.Parse(time.RFC3339Nano, value)
	return parsed, err == nil
}

// Current returns the step active when this callback context was created.
func (c *nativeContext) Current() string {
	return c.current
}

// Depth returns the number of entries in the bounded history.
func (c *nativeContext) Depth() int {
	return c.depth
}

// CanBack reports whether the flow has a previous step.
func (c *nativeContext) CanBack() bool {
	return c.canBack
}

// Step returns the active step in the runtime state.
func (c *nativeContext) Step() string {
	return c.state.CurrentStep
}

// Next requests the next step.
func (c *nativeContext) Next() StepResult {
	return stepNext
}

// Back requests the previous step.
func (c *nativeContext) Back() StepResult {
	return stepBack
}

// Go requests the named step.
func (c *nativeContext) Go(step string) StepResult {
	c.target = step
	return stepGo
}

// Stay keeps the current step.
func (c *nativeContext) Stay() StepResult {
	return stepStay
}
