package teleflow

import (
	"encoding/json"
	"errors"
	"math"
	"reflect"
	"testing"
	"time"
)

func TestSetAndGet(t *testing.T) {
	t.Parallel()

	testTime := time.Date(2026, time.September, 27, 12, 34, 56, 789, time.UTC)
	tests := []struct {
		name string
		set  func(Context) error
		get  func(Context) (any, bool)
		want any
	}{
		{
			name: "string",
			set: func(ctx Context) error {
				return ctx.SetString("value", "teleflow")
			},
			get: func(ctx Context) (any, bool) {
				return ctx.GetString("value")
			},
			want: "teleflow",
		},
		{
			name: "bool",
			set: func(ctx Context) error {
				return ctx.SetBool("value", true)
			},
			get: func(ctx Context) (any, bool) {
				return ctx.GetBool("value")
			},
			want: true,
		},
		{
			name: "int",
			set: func(ctx Context) error {
				return ctx.SetInt("value", 42)
			},
			get: func(ctx Context) (any, bool) {
				return ctx.GetInt("value")
			},
			want: 42,
		},
		{
			name: "int32",
			set: func(ctx Context) error {
				return ctx.SetInt32("value", 42)
			},
			get: func(ctx Context) (any, bool) {
				return ctx.GetInt32("value")
			},
			want: int32(42),
		},
		{
			name: "int64",
			set: func(ctx Context) error {
				return ctx.SetInt64("value", math.MaxInt64)
			},
			get: func(ctx Context) (any, bool) {
				return ctx.GetInt64("value")
			},
			want: int64(math.MaxInt64),
		},
		{
			name: "float32",
			set: func(ctx Context) error {
				return ctx.SetFloat32("value", 12.5)
			},
			get: func(ctx Context) (any, bool) {
				return ctx.GetFloat32("value")
			},
			want: float32(12.5),
		},
		{
			name: "float64",
			set: func(ctx Context) error {
				return ctx.SetFloat64("value", 12.5)
			},
			get: func(ctx Context) (any, bool) {
				return ctx.GetFloat64("value")
			},
			want: 12.5,
		},
		{
			name: "duration",
			set: func(ctx Context) error {
				return ctx.SetDuration("value", 5*time.Minute)
			},
			get: func(ctx Context) (any, bool) {
				return ctx.GetDuration("value")
			},
			want: 5 * time.Minute,
		},
		{
			name: "time",
			set: func(ctx Context) error {
				return ctx.SetTime("value", testTime)
			},
			get: func(ctx Context) (any, bool) {
				return ctx.GetTime("value")
			},
			want: testTime,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			ctx := &nativeContext{
				state: &runtimeState{},
			}
			if err := tt.set(ctx); err != nil {
				t.Fatalf("set error = %v", err)
			}

			got, ok := tt.get(ctx)
			if !ok {
				t.Fatal("get ok = false, want true")
			}
			if !reflect.DeepEqual(got, tt.want) {
				t.Fatalf("get value = %#v, want %#v", got, tt.want)
			}
		})
	}
}

func TestSetRejectsEmptyKey(t *testing.T) {
	tests := []struct {
		name string
		test func(t *testing.T)
	}{
		{
			name: "set_rejects_empty_key",
			test: func(t *testing.T) {
				t.Parallel()

				ctx := &nativeContext{
					state: &runtimeState{},
				}

				if err := ctx.SetString("", "value"); !errors.Is(err, ErrDataKeyEmpty) {
					t.Fatalf("SetString() error = %v, want %v", err, ErrDataKeyEmpty)
				}
				if ctx.state.Data != nil {
					t.Fatalf("state data = %#v, want nil", ctx.state.Data)
				}
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, tt.test)
	}
}

func TestSetRejectsUnsupportedValue(t *testing.T) {
	tests := []struct {
		name string
		test func(t *testing.T)
	}{
		{
			name: "set_rejects_unsupported_value",
			test: func(t *testing.T) {
				t.Parallel()

				ctx := &nativeContext{
					state: &runtimeState{},
				}

				err := ctx.set("value", make(chan int))
				if !errors.Is(err, ErrInvalidValue) {
					t.Fatalf("set() error = %v, want %v", err, ErrInvalidValue)
				}
				if ctx.Has("value") {
					t.Fatal("Has() = true, want false")
				}
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, tt.test)
	}
}

func TestSetFloat64RejectsNonFiniteValue(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name  string
		value float64
	}{
		{
			name:  "nan",
			value: math.NaN(),
		},
		{
			name:  "positive_infinity",
			value: math.Inf(1),
		},
		{
			name:  "negative_infinity",
			value: math.Inf(-1),
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			ctx := &nativeContext{
				state: &runtimeState{},
			}
			if err := ctx.SetFloat64("value", 12.5); err != nil {
				t.Fatalf("prepare SetFloat64() error = %v", err)
			}

			if err := ctx.SetFloat64("value", tt.value); !errors.Is(err, ErrInvalidValue) {
				t.Fatalf("SetFloat64() error = %v, want %v", err, ErrInvalidValue)
			}
			got, ok := ctx.GetFloat64("value")
			if !ok || got != 12.5 {
				t.Fatalf("GetFloat64() = %v, %t, want 12.5, true", got, ok)
			}
		})
	}
}

func TestDeleteAndHas(t *testing.T) {
	tests := []struct {
		name string
		test func(t *testing.T)
	}{
		{
			name: "delete_and_has",
			test: func(t *testing.T) {
				t.Parallel()

				ctx := &nativeContext{
					state: &runtimeState{},
				}
				if err := ctx.SetString("value", "teleflow"); err != nil {
					t.Fatalf("SetString() error = %v", err)
				}
				if !ctx.Has("value") {
					t.Fatal("Has() = false, want true")
				}

				if err := ctx.Delete("value"); err != nil {
					t.Fatalf("Delete() error = %v", err)
				}
				if ctx.Has("value") {
					t.Fatal("Has() = true, want false")
				}
				if err := ctx.Delete("value"); err != nil {
					t.Fatalf("second Delete() error = %v", err)
				}
				if err := ctx.Delete(""); !errors.Is(err, ErrDataKeyEmpty) {
					t.Fatalf("Delete() empty key error = %v, want %v", err, ErrDataKeyEmpty)
				}
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, tt.test)
	}
}

func TestGetMissingAndMismatchedValues(t *testing.T) {
	tests := []struct {
		name string
		test func(t *testing.T)
	}{
		{
			name: "get_missing_and_mismatched_values",
			test: func(t *testing.T) {
				t.Parallel()

				ctx := &nativeContext{
					state: &runtimeState{},
				}
				if _, ok := ctx.GetString("missing"); ok {
					t.Fatal("GetString() missing ok = true, want false")
				}
				if err := ctx.SetString("value", "teleflow"); err != nil {
					t.Fatalf("SetString() error = %v", err)
				}
				if _, ok := ctx.GetBool("value"); ok {
					t.Fatal("GetBool() mismatched type ok = true, want false")
				}

				ctx.state.Data["invalid"] = json.RawMessage("{")
				if _, ok := ctx.GetString("invalid"); ok {
					t.Fatal("GetString() invalid JSON ok = true, want false")
				}
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, tt.test)
	}
}

func TestGetInt32Range(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name  string
		value int64
		want  int32
		ok    bool
	}{
		{
			name:  "minimum",
			value: math.MinInt32,
			want:  math.MinInt32,
			ok:    true,
		},
		{
			name:  "maximum",
			value: math.MaxInt32,
			want:  math.MaxInt32,
			ok:    true,
		},
		{
			name:  "below_minimum",
			value: math.MinInt32 - 1,
		},
		{
			name:  "above_maximum",
			value: math.MaxInt32 + 1,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			ctx := &nativeContext{
				state: &runtimeState{},
			}
			if err := ctx.SetInt64("value", tt.value); err != nil {
				t.Fatalf("SetInt64() error = %v", err)
			}

			got, ok := ctx.GetInt32("value")
			if ok != tt.ok || got != tt.want {
				t.Fatalf("GetInt32() = %d, %t, want %d, %t", got, ok, tt.want, tt.ok)
			}
		})
	}
}

func TestGetFloat32Range(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name  string
		value float64
		want  float32
		ok    bool
	}{
		{
			name:  "finite_value",
			value: 12.5,
			want:  12.5,
			ok:    true,
		},
		{
			name:  "positive_overflow",
			value: math.MaxFloat64,
		},
		{
			name:  "negative_overflow",
			value: -math.MaxFloat64,
		},
		{
			name:  "positive_underflow",
			value: math.SmallestNonzeroFloat64,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			ctx := &nativeContext{
				state: &runtimeState{},
			}
			if err := ctx.SetFloat64("value", tt.value); err != nil {
				t.Fatalf("SetFloat64() error = %v", err)
			}

			got, ok := ctx.GetFloat32("value")
			if ok != tt.ok || got != tt.want {
				t.Fatalf("GetFloat32() = %v, %t, want %v, %t", got, ok, tt.want, tt.ok)
			}
		})
	}
}

func TestGetTimeRejectsInvalidValue(t *testing.T) {
	tests := []struct {
		name string
		test func(t *testing.T)
	}{
		{
			name: "get_time_rejects_invalid_value",
			test: func(t *testing.T) {
				t.Parallel()

				ctx := &nativeContext{
					state: &runtimeState{},
				}
				if err := ctx.SetString("value", "not-a-time"); err != nil {
					t.Fatalf("SetString() error = %v", err)
				}

				got, ok := ctx.GetTime("value")
				if ok || !got.IsZero() {
					t.Fatalf("GetTime() = %v, %t, want zero, false", got, ok)
				}
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, tt.test)
	}
}

func TestMetadataAndTransitions(t *testing.T) {
	tests := []struct {
		name string
		test func(t *testing.T)
	}{
		{
			name: "metadata_and_transitions",
			test: func(t *testing.T) {
				t.Parallel()

				ctx := &nativeContext{
					state: &runtimeState{
						CurrentStep: "payment",
					},
					current: "amount",
					depth:   3,
					canBack: true,
				}

				if got := ctx.Current(); got != "amount" {
					t.Fatalf("Current() = %q, want %q", got, "amount")
				}
				if got := ctx.Step(); got != "payment" {
					t.Fatalf("Step() = %q, want %q", got, "payment")
				}
				if got := ctx.Depth(); got != 3 {
					t.Fatalf("Depth() = %d, want 3", got)
				}
				if !ctx.CanBack() {
					t.Fatal("CanBack() = false, want true")
				}
				if got := ctx.Next(); got != stepNext {
					t.Fatalf("Next() = %d, want %d", got, stepNext)
				}
				if got := ctx.Back(); got != stepBack {
					t.Fatalf("Back() = %d, want %d", got, stepBack)
				}
				if got := ctx.Go("receipt"); got != stepGo {
					t.Fatalf("Go() = %d, want %d", got, stepGo)
				}
				if ctx.target != "receipt" {
					t.Fatalf("Go() target = %q, want %q", ctx.target, "receipt")
				}
				if got := ctx.Stay(); got != stepStay {
					t.Fatalf("Stay() = %d, want %d", got, stepStay)
				}
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, tt.test)
	}
}
