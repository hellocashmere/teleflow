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
	ErrTargetStepNotFound      = errors.New("flow: target step not found")
)

// Bus registers flow definitions and processes their sessions.
//
// It coordinates concurrent updates only within one Bus instance.
type Bus interface {
	// NewFlow creates a mutable flow draft.
	//
	// Add steps and call Build before passing the flow to Start.
	//
	// Example:
	//
	//	flow := bus.NewFlow(FlowConfig{
	//		Name:    "signup",
	//		Version: 1,
	//	})
	NewFlow(config FlowConfig) *Flow

	// Start creates a session for a built flow.
	//
	// The flow must have been created by this Bus and successfully built.
	// If a session already exists, Start resumes the stored session's pending begin callback and otherwise leaves it unchanged.
	// An expired session is removed and reported as ErrExpired.
	//
	// Example:
	//
	//	if err := flow.Build(); err != nil {
	//		log.Fatal(err)
	//	}
	//
	//	bot.Handle("/signup", func(c tele.Context) error {
	//		return bus.Start(ctx, c, flow)
	//	})
	Start(ctx context.Context, c tele.Context, flow *Flow) error

	// Handle processes an update for an active session using context.Background.
	//
	// Register it for every Telebot event used by the flow.
	// Updates without an active session or a matching current-step event are ignored.
	//
	// Example:
	//
	//	bot.Handle(tele.OnText, bus.Handle)
	Handle(c tele.Context) error

	// HandleCtx returns a Telebot handler that uses ctx for every update.
	//
	// Register the returned handler for every Telebot event used by the flow.
	//
	// Example:
	//
	//	handler := bus.HandleCtx(ctx)
	//	bot.Handle(tele.OnText, handler)
	//	bot.Handle(tele.OnCallback, handler)
	HandleCtx(ctx context.Context) tele.HandlerFunc

	// Cancel removes the active session identified by the Telebot context.
	//
	// It succeeds when no session exists.
	//
	// Example:
	//
	//	bot.Handle("/cancel", func(c tele.Context) error {
	//		return bus.Cancel(ctx, c)
	//	})
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

// NewBus creates a flow bus backed by store.
//
// A nil store is accepted, but session operations then return ErrStorageNil.
//
// The bus serializes updates for each session within this Bus instance.
// Other Bus instances are not coordinated, even when they share a store.
func NewBus(store storage.Storage) Bus {
	return &bus{
		store:     store,
		defs:      map[string]*Flow{},
		now:       time.Now,
		userLocks: map[string]*keyedLock{},
	}
}

// NewFlow returns a mutable flow draft.
//
// Call Build to validate and register it before starting a session.
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

// Start creates a session for a flow created and built by this Bus.
//
// If a session already exists, Start retries the stored session's pending begin callback and otherwise leaves it unchanged.
// An expired session is removed and reported as ErrExpired.
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

	if err := b.persist(ctx, key, state, f.idleTimeout); err != nil {
		return fmt.Errorf("flow: failed to set state: %w", err)
	}

	return b.resumeBegin(ctx, key, c, state, f)
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
		if err = b.store.Delete(ctx, key); err != nil {
			return fmt.Errorf("flow: delete expired state: %w", err)
		}

		return ErrExpired
	}

	if state.PendingBegin {
		return b.resumeBegin(ctx, key, c, state, f)
	}

	return nil
}

// Handle dispatches an inferred Telebot event with a background context.
//
// Use HandleCtx when storage operations and step callbacks must observe cancellation or deadlines.
func (b *bus) Handle(c tele.Context) error {
	return b.handle(context.Background(), c)
}

// HandleCtx returns a Telebot handler that uses ctx for every update.
func (b *bus) HandleCtx(ctx context.Context) tele.HandlerFunc {
	return func(c tele.Context) error {
		return b.handle(ctx, c)
	}
}

func (b *bus) handle(ctx context.Context, c tele.Context) error {
	if b.store == nil {
		return ErrStorageNil
	}

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
		if deleteErr := b.store.Delete(ctx, key); deleteErr != nil {
			return fmt.Errorf("flow: delete expired state: %w", deleteErr)
		}

		return ErrExpired
	}

	if state.PendingBegin {
		return b.resumeBegin(ctx, key, c, state, f)
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

	working := cloneRuntime(state)
	fc := &nativeContext{
		state:   working,
		current: working.CurrentStep,
		depth:   len(working.History),
		canBack: len(working.History) > 1,
	}

	var result StepResult
	if step.handle != nil {
		result, err = step.handle(ctx, c, fc)
	}
	if err != nil {
		return fmt.Errorf("flow: step %q: %w", state.CurrentStep, err)
	}

	state = working
	state.LastActivity = b.currentTime()
	state.LastUpdateID = c.Update().ID

	switch result {
	case stepStay:
		return b.persist(ctx, key, state, f.idleTimeout)
	case stepBack:
		if len(state.History) <= 1 {
			return b.persist(ctx, key, state, f.idleTimeout)
		}

		state.History = state.History[:len(state.History)-1]
		state.CurrentStep = state.History[len(state.History)-1]
		state.PendingBegin = true

		if err := b.persist(ctx, key, state, f.idleTimeout); err != nil {
			return err
		}
		if err := b.resumeBegin(ctx, key, c, state, f); err != nil {
			return err
		}
		return nil
	case stepNext:
		next := f.next(state.CurrentStep)
		if next == nil {
			if err := b.store.Delete(ctx, key); err != nil {
				return fmt.Errorf("flow: delete completed state: %w", err)
			}
			return nil
		}

		state.CurrentStep = next.name
		state.History = appendBounded(state.History, next.name, f.historyLimit)
		state.PendingBegin = true

		if err := b.persist(ctx, key, state, f.idleTimeout); err != nil {
			return err
		}
		return b.resumeBegin(ctx, key, c, state, f)
	case stepGo:
		target := f.step(fc.target)
		if target == nil {
			return fmt.Errorf("%w: %q", ErrTargetStepNotFound, fc.target)
		}

		state.CurrentStep = target.name
		state.History = appendBounded(state.History, target.name, f.historyLimit)
		state.PendingBegin = true

		if err := b.persist(ctx, key, state, f.idleTimeout); err != nil {
			return err
		}

		return b.resumeBegin(ctx, key, c, state, f)
	default:
		return fmt.Errorf("%w: %d", ErrInvalidStepResult, result)
	}
}

// Cancel removes the active session identified by the Telebot context.
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

	if err := b.store.Delete(ctx, key); err != nil {
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
) error {
	step := f.step(state.CurrentStep)
	if step == nil {
		return fmt.Errorf("flow: step %q not found", state.CurrentStep)
	}

	if step.begin != nil {
		err := step.begin(ctx, c)
		if err != nil {
			return fmt.Errorf("flow: begin step %q: %w", state.CurrentStep, err)
		}
	}

	state.PendingBegin = false
	state.LastActivity = b.currentTime()

	if err := b.persist(ctx, key, state, f.idleTimeout); err != nil {
		return fmt.Errorf("flow: persist entered step %q: %w", state.CurrentStep, err)
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
	state *runtimeState,
	expiration time.Duration,
) error {
	value, err := encodeRuntime(state)
	if err != nil {
		return err
	}

	if err := b.store.Set(ctx, key, value, expiration); err != nil {
		return fmt.Errorf("flow: set state: %w", err)
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
	return !rs.LastActivity.IsZero() && !now.Before(rs.LastActivity.Add(f.idleTimeout))
}

func (b *bus) currentTime() time.Time {
	if b.now != nil {
		return b.now()
	}

	return time.Now()
}

// sessionKey identifies a session by sender and, when available, chat and message thread.
// It returns ErrContextNoSender when the context or sender is missing.
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

func defKey(n string, v uint) string {
	return n + "\x00" + strconv.FormatUint(uint64(v), 10)
}
