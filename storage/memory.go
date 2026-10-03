package storage

import (
	"context"
	"errors"
	"sync"
	"time"
)

var ErrKeyNotFound = errors.New("key not found")

// Memory is a concurrent, process-local Storage.
// Its zero value is ready for use.
type Memory struct {
	data sync.Map
}

type raw struct {
	value     []byte
	expiresAt time.Time
	timerMu   sync.Mutex
	timer     *time.Timer
	isRemoved bool
}

// NewMemory returns an empty, process-local Storage.
func NewMemory() Storage {
	return &Memory{}
}

// Get returns an independent copy of the value associated with key.
//
// It returns ErrKeyNotFound when the key is missing or expired.
// If ctx is already done, Get returns ctx.Err without reading storage.
func (m *Memory) Get(ctx context.Context, key string) ([]byte, error) {
	if err := ctx.Err(); err != nil {
		return nil, err
	}

	value, ok := m.data.Load(key)
	if !ok {
		return nil, ErrKeyNotFound
	}

	v := value.(*raw)
	if m.isExpired(v) {
		if m.data.CompareAndDelete(key, v) {
			v.stopTimer()
		}
		return nil, ErrKeyNotFound
	}

	return append([]byte(nil), v.value...), nil
}

// Set replaces key with a copy of value.
//
// A positive expiration removes the value after that duration, while a zero or negative expiration keeps it until it is replaced or deleted.
// If ctx is already done, Set returns ctx.Err without changing storage.
func (m *Memory) Set(ctx context.Context, key string, value []byte, expiration time.Duration) error {
	if err := ctx.Err(); err != nil {
		return err
	}

	v := m.newRaw(value, expiration)
	previous, replaced := m.data.Swap(key, v)
	v.startTimer(m, key, expiration)
	if replaced {
		previous.(*raw).stopTimer()
	}
	return nil
}

// Delete removes key and returns nil when it is absent.
//
// If ctx is already done, Delete returns ctx.Err without changing storage.
func (m *Memory) Delete(ctx context.Context, key string) error {
	if err := ctx.Err(); err != nil {
		return err
	}
	if value, ok := m.data.LoadAndDelete(key); ok {
		value.(*raw).stopTimer()
	}
	return nil
}

func (m *Memory) newRaw(value []byte, expiration time.Duration) *raw {
	v := &raw{value: append([]byte(nil), value...)}
	if expiration > 0 {
		v.expiresAt = time.Now().Add(expiration)
	}
	return v
}

func (m *Memory) isExpired(v *raw) bool {
	return !v.expiresAt.IsZero() && !time.Now().Before(v.expiresAt)
}

func (v *raw) startTimer(m *Memory, key string, expiration time.Duration) {
	if expiration <= 0 {
		return
	}

	v.timerMu.Lock()
	defer v.timerMu.Unlock()
	if v.isRemoved {
		return
	}
	v.timer = time.AfterFunc(expiration, func() {
		if m.data.CompareAndDelete(key, v) {
			v.stopTimer()
		}
	})
}

func (v *raw) stopTimer() {
	v.timerMu.Lock()
	defer v.timerMu.Unlock()
	v.isRemoved = true
	if v.timer != nil {
		v.timer.Stop()
		v.timer = nil
	}
}
