package relay

import (
	"context"
	"sync"
)

// MultiFrontend fans multiple frontend Endpoints (e.g. Telegram + Discord)
// into the single Frontend slot the Broker knows how to drive. The Broker
// itself stays platform-agnostic; MultiFrontend is just wiring glue so
// cmd/relayd can actually start more than one frontend at once — see
// internal/endpoint/discord/DESIGN.md §9, "wiring is where the real
// integration happens, not a follow-up."
//
// Routing outbound Send calls back to the right underlying frontend is the
// hard part: a relayd-originated message (scheduler reminder, admin notice,
// mcp reply tool call) generally carries only ConversationID/chat_id, with
// no marker saying which platform it belongs to. MultiFrontend remembers,
// for every ConversationID it has ever seen delivered INBOUND from a given
// sub-frontend, which one that was, and routes outbound Sends for that
// ConversationID back to the same one. A ConversationID never seen inbound
// (rare - would mean sending an unprompted message to someone who has never
// messaged the bot at all, e.g. a hardcoded admin id before their first
// message this process lifetime) falls back to the first-registered
// frontend, preserving pre-multi-frontend behavior for the primary/admin
// platform.
type MultiFrontend struct {
	frontends []Endpoint

	mu    sync.RWMutex
	owner map[string]Endpoint

	recv chan Message
}

// NewMultiFrontend fans in the Recv() channel of every given frontend and
// returns a single Endpoint that can stand in for Broker.Frontend. at least
// one frontend must be given.
func NewMultiFrontend(frontends ...Endpoint) *MultiFrontend {
	mf := &MultiFrontend{
		frontends: frontends,
		owner:     map[string]Endpoint{},
		recv:      make(chan Message, 64),
	}
	var wg sync.WaitGroup
	for _, fe := range frontends {
		wg.Add(1)
		go func(fe Endpoint) {
			defer wg.Done()
			for m := range fe.Recv() {
				mf.mu.Lock()
				mf.owner[m.ConversationID] = fe
				mf.mu.Unlock()
				mf.recv <- m
			}
		}(fe)
	}
	go func() {
		wg.Wait()
		close(mf.recv)
	}()
	return mf
}

func (mf *MultiFrontend) Name() string         { return "multi" }
func (mf *MultiFrontend) Recv() <-chan Message { return mf.recv }

// Send routes to whichever sub-frontend last delivered an inbound message
// for m.ConversationID, or the first-registered frontend if none has.
func (mf *MultiFrontend) Send(ctx context.Context, m Message) error {
	mf.mu.RLock()
	fe, ok := mf.owner[m.ConversationID]
	mf.mu.RUnlock()
	if !ok {
		fe = mf.frontends[0]
	}
	return fe.Send(ctx, m)
}

// Close closes every sub-frontend, collecting the first error but always
// attempting all of them so one broken frontend doesn't leak another's
// connection.
func (mf *MultiFrontend) Close() error {
	var firstErr error
	for _, fe := range mf.frontends {
		if err := fe.Close(); err != nil && firstErr == nil {
			firstErr = err
		}
	}
	return firstErr
}
