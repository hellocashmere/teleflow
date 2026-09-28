package teleflow

import (
	"context"
	"errors"
	"fmt"
	"strconv"
	"sync"
	"time"

	"github.com/hellocashmere/teleflow/storage"
	tele "gopkg.in/telebot.v4"
)

const (
	operationLeaseTimeout = time.Minute
	releaseTimeout        = 5 * time.Second
)

var (
	ErrNameEmpty               = errors.New("flow: name cannot be empty")
	ErrVersionInvalid          = errors.New("flow: version must be greater than zero")
	ErrIdleTimeoutNegative     = errors.New("flow: idle timeout cannot be negative")
	ErrHistoryLimitNegative    = errors.New("flow: history limit cannot be negative")
	ErrNilDefinition           = errors.New("flow: nil definition")
	ErrNoStepsDefined          = errors.New("flow: no steps defined")
	ErrDefinitionExists        = errors.New("flow: definition already exists")
	ErrExpired                 = errors.New("flow: expired")
	ErrDefinitionNotRegistered = errors.New("flow: definition is not registered")
	ErrInvalidRuntimeState     = errors.New("flow: invalid runtime state")
	ErrContextNoSender         = errors.New("flow: context has no sender")
	ErrInvalidStepResult       = errors.New("flow: invalid step result")
	ErrStorageNil              = errors.New("flow: storage is nil")
	ErrConcurrentUpdate        = errors.New("flow: state changed concurrently")
	ErrStateBusy               = errors.New("flow: state is being processed")
	ErrTargetStepNotFound      = errors.New("flow: target step not found")
)

type Bus interface {
	// NewFlow creates a versioned flow draft.
	NewFlow(config FlowConfig) *Flow

	// Start starts a flow or resumes its pending begin callback.
	Start(ctx context.Context, c tele.Context, flow *Flow) error

	// Handle processes an inferred Telebot event.
	Handle(c tele.Context) error

	// Cancel removes the active flow for the current chat and sender.
	Cancel(ctx context.Context, c tele.Context) error
}

type bus struct {
	store storage.Storage
	defs  map[string]*Flow
	now   func() time.Time

	defsMu    sync.RWMutex
	locksMu   sync.Mutex
	userLocks map[string]*keyedLock
}

type keyedLock struct {
	mu   sync.Mutex
	refs int
}

// NewBus creates [bus].
func NewBus(store storage.Storage) Bus {
	return &bus{
		store:     store,
		defs:      map[string]*Flow{},
		now:       time.Now,
		userLocks: map[string]*keyedLock{},
	}
}

// NewFlow returns a mutable flow draft. Build validates and registers it.
func (b *bus) NewFlow(cfg FlowConfig) *Flow {
	return &Flow{
		owner:        b,
		name:         cfg.Name,
		version:      cfg.Version,
		idleTimeout:  cfg.IdleTimeout,
		historyLimit: cfg.HistoryLimit,
		steps:        []*Step{},
	}
}

func (b *bus) register(f *Flow) error {
	k := defKey(f.name, f.version)
	b.defsMu.Lock()
	defer b.defsMu.Unlock()

	if registered, exists := b.defs[k]; exists {
		if registered == f {
			return nil
		}

		return fmt.Errorf("%w: %q version %d", ErrDefinitionExists, f.name, f.version)
	}

	b.defs[k] = f

	return nil
}

// Start starts a flow or resumes its pending begin callback.
func (b *bus) Start(ctx context.Context, c tele.Context, f *Flow) error {
	if b.store == nil {
		return ErrStorageNil
	}

	if f == nil {
		return ErrNilDefinition
	}

	if err := f.ready(); err != nil {
		return fmt.Errorf("flow %q: %w", f.name, err)
	}

	if registered := b.lookup(f.name, f.version); registered != f {
		return ErrDefinitionNotRegistered
	}

	key, err := sessionKey(c)
	if err != nil {
		return err
	}

	unlock := b.lockFor(key)
	defer unlock()

	for attempt := 0; attempt < 2; attempt++ {
		stored, getErr := b.store.Get(ctx, key)
		if getErr == nil {
			return b.startStored(ctx, key, c, stored)
		}

		if !errors.Is(getErr, storage.ErrKeyNotFound) {
			return fmt.Errorf("flow: get stored state: %w", getErr)
		}

		now := b.currentTime()
		first := f.first()
		if first == nil {
			return ErrNoStepsDefined
		}

		state := &runtimeState{
			FlowName:     f.name,
			FlowVersion:  f.version,
			CurrentStep:  first.name,
			History:      []string{first.name},
			StartedAt:    now,
			LastActivity: now,
			PendingBegin: true,
		}

		created, persistErr := b.persist(ctx, key, nil, state, f.idleTimeout)
		if errors.Is(persistErr, ErrConcurrentUpdate) {
			continue
		}

		if persistErr != nil {
			return fmt.Errorf("flow: failed to set state: %w", persistErr)
		}

		return b.resumeBegin(ctx, key, c, state, f, created)
	}

	return ErrConcurrentUpdate
}

func (b *bus) startStored(ctx context.Context, key string, c tele.Context, stored []byte) error {
	state, err := decodeRuntime(stored)
	if err != nil {
		return fmt.Errorf("flow: decode stored state: %w", err)
	}

	f, err := b.definitionFor(state)
	if err != nil {
		return err
	}

	if b.expired(state, f) {
		if err = b.delete(ctx, key, stored); err != nil {
			return fmt.Errorf("flow: delete expired state: %w", err)
		}

		return ErrExpired
	}

	if state.PendingBegin {
		return b.resumeBegin(ctx, key, c, state, f, stored)
	}

	return nil
}

// Handle dispatches an inferred Telebot event.
func (b *bus) Handle(c tele.Context) error {
	if b.store == nil {
		return ErrStorageNil
	}

	ctx := context.Background()
	event := b.getEventType(c)

	key, err := sessionKey(c)
	if err != nil {
		return nil
	}

	unlock := b.lockFor(key)
	defer unlock()

	v, err := b.store.Get(ctx, key)
	if errors.Is(err, storage.ErrKeyNotFound) {
		return nil
	}

	if err != nil {
		return fmt.Errorf("flow: get stored value: %w", err)
	}

	state, err := decodeRuntime(v)
	if err != nil {
		return fmt.Errorf("flow: decode stored state: %w", err)
	}

	f, err := b.definitionFor(state)
	if err != nil {
		return err
	}

	if b.expired(state, f) {
		if deleteErr := b.delete(ctx, key, v); deleteErr != nil {
			return fmt.Errorf("flow: delete expired state: %w", deleteErr)
		}

		return ErrExpired
	}

	if state.PendingBegin {
		return b.resumeBegin(ctx, key, c, state, f, v)
	}

	if updateID := c.Update().ID; updateID > 0 && updateID <= state.LastUpdateID {
		return nil
	}

	step := f.step(state.CurrentStep)
	if step == nil {
		return fmt.Errorf("flow: step %q not found", state.CurrentStep)
	}

	if event != step.on {
		return nil
	}

	claimed, claimedValue, err := b.claim(ctx, key, v, state, f.idleTimeout)
	if err != nil {
		return err
	}

	working := cloneRuntime(claimed)
	fc := &nativeContext{
		state:   working,
		current: working.CurrentStep,
		depth:   len(working.History),
		canBack: len(working.History) > 1,
	}

	var result StepResult
	if step.handle != nil {
		callbackCtx, cancel := leaseContext(ctx, claimed.LeaseUntil)
		result, err = step.handle(callbackCtx, c, fc)
		cancel()
	}
	if err != nil {
		releaseErr := b.release(ctx, key, claimedValue, claimed, f.idleTimeout)

		return errors.Join(fmt.Errorf("flow: step %q: %w", state.CurrentStep, err), releaseErr)
	}

	state = working
	state.LastActivity = b.currentTime()
	state.LastUpdateID = c.Update().ID
	state.LeaseUntil = time.Time{}

	switch result {
	case stepStay:
		_, err = b.persist(ctx, key, claimedValue, state, f.idleTimeout)
		return err
	case stepBack:
		if len(state.History) <= 1 {
			_, err = b.persist(ctx, key, claimedValue, state, f.idleTimeout)
			return err
		}

		state.History = state.History[:len(state.History)-1]
		state.CurrentStep = state.History[len(state.History)-1]
		state.PendingBegin = true

		pending, persistErr := b.persist(ctx, key, claimedValue, state, f.idleTimeout)
		if persistErr != nil {
			return persistErr
		}
		if err := b.resumeBegin(ctx, key, c, state, f, pending); err != nil {
			return err
		}
		return nil
	case stepNext:
		next := f.next(state.CurrentStep)
		if next == nil {
			if err := b.delete(ctx, key, claimedValue); err != nil {
				return fmt.Errorf("flow: delete completed state: %w", err)
			}
			return nil
		}

		state.CurrentStep = next.name
		state.History = appendBounded(state.History, next.name, f.historyLimit)
		state.PendingBegin = true

		pending, persistErr := b.persist(ctx, key, claimedValue, state, f.idleTimeout)
		if persistErr != nil {
			return persistErr
		}
		return b.resumeBegin(ctx, key, c, state, f, pending)
	case stepGo:
		target := f.step(fc.target)
		if target == nil {
			releaseErr := b.release(ctx, key, claimedValue, claimed, f.idleTimeout)

			return errors.Join(
				fmt.Errorf("%w: %q", ErrTargetStepNotFound, fc.target),
				releaseErr,
			)
		}

		state.CurrentStep = target.name
		state.History = appendBounded(state.History, target.name, f.historyLimit)
		state.PendingBegin = true

		pending, persistErr := b.persist(ctx, key, claimedValue, state, f.idleTimeout)
		if persistErr != nil {
			return persistErr
		}

		return b.resumeBegin(ctx, key, c, state, f, pending)
	default:
		releaseErr := b.release(ctx, key, claimedValue, claimed, f.idleTimeout)
		return errors.Join(fmt.Errorf("%w: %d", ErrInvalidStepResult, result), releaseErr)
	}
}

func (b *bus) Cancel(ctx context.Context, c tele.Context) error {
	if b.store == nil {
		return ErrStorageNil
	}

	key, err := sessionKey(c)
	if err != nil {
		return err
	}

	unlock := b.lockFor(key)
	defer unlock()

	stored, err := b.store.Get(ctx, key)
	if errors.Is(err, storage.ErrKeyNotFound) {
		return nil
	}

	if err != nil {
		return fmt.Errorf("flow: cancel: %w", err)
	}

	if err := b.delete(ctx, key, stored); err != nil {
		return fmt.Errorf("flow: cancel: %w", err)
	}

	return nil
}

func (b *bus) resumeBegin(
	ctx context.Context,
	key string,
	c tele.Context,
	state *runtimeState,
	f *Flow,
	stored []byte,
) error {
	claimed, claimedValue, err := b.claim(ctx, key, stored, state, f.idleTimeout)
	if err != nil {
		return err
	}

	step := f.step(claimed.CurrentStep)
	if step == nil {
		releaseErr := b.release(ctx, key, claimedValue, claimed, f.idleTimeout)

		return errors.Join(fmt.Errorf("flow: step %q not found", claimed.CurrentStep), releaseErr)
	}

	if step.begin != nil {
		callbackCtx, cancel := leaseContext(ctx, claimed.LeaseUntil)
		err := step.begin(callbackCtx, c)
		cancel()
		if err != nil {
			releaseErr := b.release(ctx, key, claimedValue, claimed, f.idleTimeout)

			return errors.Join(fmt.Errorf("flow: begin step %q: %w", claimed.CurrentStep, err), releaseErr)
		}
	}

	claimed.PendingBegin = false
	claimed.LastActivity = b.currentTime()
	claimed.LeaseUntil = time.Time{}

	if _, err := b.persist(ctx, key, claimedValue, claimed, f.idleTimeout); err != nil {
		return fmt.Errorf("flow: persist entered step %q: %w", claimed.CurrentStep, err)
	}

	return nil
}

func (b *bus) definitionFor(state *runtimeState) (*Flow, error) {
	f := b.lookup(state.FlowName, state.FlowVersion)
	if f == nil {
		return nil, fmt.Errorf("flow: definition %s/%d not found", state.FlowName, state.FlowVersion)
	}

	if err := f.ready(); err != nil {
		return nil, fmt.Errorf("flow %q: %w", f.name, err)
	}

	if err := f.validateRuntime(state); err != nil {
		return nil, fmt.Errorf("flow %q: %w", f.name, err)
	}

	return f, nil
}

func (b *bus) persist(
	ctx context.Context,
	key string,
	expected []byte,
	state *runtimeState,
	expiration time.Duration,
) ([]byte, error) {
	value, err := encodeRuntime(state)
	if err != nil {
		return nil, err
	}

	swapped, swapErr := b.store.CompareAndSwap(ctx, key, expected, value, expiration)
	if swapErr != nil {
		return nil, fmt.Errorf("flow: compare and swap state: %w", swapErr)
	}

	if !swapped {
		return nil, ErrConcurrentUpdate
	}

	return value, nil
}

func (b *bus) claim(
	ctx context.Context,
	key string,
	expected []byte,
	state *runtimeState,
	expiration time.Duration,
) (*runtimeState, []byte, error) {
	if state.LeaseUntil.After(b.currentTime()) {
		return nil, nil, ErrStateBusy
	}

	if state.Revision == ^uint64(0) {
		return nil, nil, ErrInvalidRuntimeState
	}

	claimed := cloneRuntime(state)
	claimed.Revision++
	claimed.LeaseUntil = b.currentTime().Add(operationLeaseTimeout)

	claimExpiration := expiration
	if claimExpiration < operationLeaseTimeout {
		claimExpiration = operationLeaseTimeout
	}

	value, err := b.persist(ctx, key, expected, claimed, claimExpiration)
	if err != nil {
		return nil, nil, err
	}

	return claimed, value, nil
}

func (b *bus) release(
	ctx context.Context,
	key string,
	expected []byte,
	state *runtimeState,
	expiration time.Duration,
) error {
	released := cloneRuntime(state)
	released.LeaseUntil = time.Time{}

	releaseCtx, cancel := context.WithTimeout(context.WithoutCancel(ctx), releaseTimeout)
	defer cancel()

	_, err := b.persist(releaseCtx, key, expected, released, expiration)
	if errors.Is(err, ErrConcurrentUpdate) {
		return nil
	}

	return err
}

func (b *bus) delete(ctx context.Context, key string, expected []byte) error {
	deleted, err := b.store.CompareAndDelete(ctx, key, expected)
	if err != nil {
		return fmt.Errorf("flow: compare and delete state: %w", err)
	}

	if !deleted {
		return ErrConcurrentUpdate
	}

	return nil
}

func (b *bus) getEventType(c tele.Context) string {
	if c == nil {
		return ""
	}

	if c.Callback() != nil {
		return tele.OnCallback
	}

	u := c.Update()
	switch {
	case u.EditedMessage != nil:
		return tele.OnEdited
	case u.ChannelPost != nil:
		return tele.OnChannelPost
	case u.EditedChannelPost != nil:
		return tele.OnEditedChannelPost
	case u.Query != nil:
		return tele.OnQuery
	case u.InlineResult != nil:
		return tele.OnInlineResult
	case u.ShippingQuery != nil:
		return tele.OnShipping
	case u.PreCheckoutQuery != nil:
		return tele.OnCheckout
	case u.Poll != nil:
		return tele.OnPoll
	case u.PollAnswer != nil:
		return tele.OnPollAnswer
	case u.MyChatMember != nil:
		return tele.OnMyChatMember
	case u.ChatMember != nil:
		return tele.OnChatMember
	case u.ChatJoinRequest != nil:
		return tele.OnChatJoinRequest
	case u.Boost != nil:
		return tele.OnBoost
	case u.BoostRemoved != nil:
		return tele.OnBoostRemoved
	case u.BusinessConnection != nil:
		return tele.OnBusinessConnection
	case u.BusinessMessage != nil:
		return tele.OnBusinessMessage
	case u.EditedBusinessMessage != nil:
		return tele.OnEditedBusinessMessage
	case u.DeletedBusinessMessages != nil:
		return tele.OnDeletedBusinessMessages
	}

	m := c.Message()
	if m == nil {
		return ""
	}

	switch {
	case m.Photo != nil:
		return tele.OnPhoto
	case m.Voice != nil:
		return tele.OnVoice
	case m.Audio != nil:
		return tele.OnAudio
	case m.Animation != nil:
		return tele.OnAnimation
	case m.Document != nil:
		return tele.OnDocument
	case m.Sticker != nil:
		return tele.OnSticker
	case m.Video != nil:
		return tele.OnVideo
	case m.VideoNote != nil:
		return tele.OnVideoNote
	case m.Contact != nil:
		return tele.OnContact
	case m.Venue != nil:
		return tele.OnVenue
	case m.Location != nil:
		return tele.OnLocation
	case m.Game != nil:
		return tele.OnGame
	case m.Dice != nil:
		return tele.OnDice
	case m.Invoice != nil:
		return tele.OnInvoice
	case m.Payment != nil:
		return tele.OnPayment
	case m.RefundedPayment != nil:
		return tele.OnRefund
	case m.Poll != nil:
		return tele.OnPoll
	case m.Text != "":
		return tele.OnText
	}

	return ""
}

func (b *bus) lockFor(k string) func() {
	b.locksMu.Lock()
	l := b.userLocks[k]
	if l == nil {
		l = &keyedLock{}
		b.userLocks[k] = l
	}

	l.refs++
	b.locksMu.Unlock()

	l.mu.Lock()
	return func() {
		l.mu.Unlock()

		b.locksMu.Lock()
		l.refs--

		if l.refs == 0 {
			delete(b.userLocks, k)
		}

		b.locksMu.Unlock()
	}
}

func (b *bus) lookup(n string, v uint) *Flow {
	b.defsMu.RLock()
	defer b.defsMu.RUnlock()

	return b.defs[defKey(n, v)]
}

func (b *bus) expired(rs *runtimeState, f *Flow) bool {
	now := b.currentTime()
	if rs.LeaseUntil.After(now) {
		return false
	}

	return !rs.LastActivity.IsZero() && !now.Before(rs.LastActivity.Add(f.idleTimeout))
}

func (b *bus) currentTime() time.Time {
	if b.now != nil {
		return b.now()
	}

	return time.Now()
}

func sessionKey(c tele.Context) (string, error) {
	if c == nil || c.Sender() == nil {
		return "", ErrContextNoSender
	}

	key := "user:" + strconv.FormatInt(c.Sender().ID, 10)
	if chat := c.Chat(); chat != nil {
		key = "chat:" + strconv.FormatInt(chat.ID, 10) + ":" + key

		if threadID := c.ThreadID(); threadID != 0 {
			key += ":thread:" + strconv.Itoa(threadID)
		}
	}

	return key, nil
}

func appendBounded(h []string, id string, limit int) []string {
	h = append(h, id)

	if limit <= 0 {
		limit = DefaultHistoryLimit
	}

	if len(h) > limit {
		h = append([]string(nil), h[len(h)-limit:]...)
	}

	return h
}

func leaseContext(ctx context.Context, until time.Time) (context.Context, context.CancelFunc) {
	if until.IsZero() {
		return context.WithCancel(ctx)
	}

	return context.WithDeadline(ctx, until)
}

func defKey(n string, v uint) string {
	return n + "\x00" + strconv.FormatUint(uint64(v), 10)
}
