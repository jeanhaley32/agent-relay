package relay

import (
	"context"
	"sync"
)

// Claimer is an optional interface a sub-frontend can implement to say
// whether a given ConversationID plausibly belongs to it, independent of
// whether it has ever seen that id delivered inbound this process lifetime.
// MultiFrontend consults it as a fallback for the "never seen inbound" case
// (see Send below) so a relayd-originated message (a scheduled reminder
// firing before the recipient has ever messaged the bot this run, an admin
// notice, etc.) still lands on the right platform instead of always going to
// frontends[0].
type Claimer interface {
	OwnsConversationID(id string) bool
}

// MultiFrontend fans multiple frontend Endpoints (e.g. Telegram + Discord)
// into the single Frontend slot the Broker knows how to drive, keeping the
// Broker itself platform-agnostic and letting callers run more than one
// frontend at once without the Broker knowing multiple exist.
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

	mu sync.RWMutex
	// owner grows one entry per distinct ConversationID ever seen inbound
	// and is never purged, but it's bounded by the number of distinct
	// conversations rather than by message volume, so it isn't a practical
	// leak.
	owner map[string]Endpoint

	recv chan Message
}

// NewMultiFrontend fans in the Recv() channel of every given frontend and
// returns a single Endpoint that can stand in for Broker.Frontend. It
// panics if no frontends are given.
func NewMultiFrontend(frontends ...Endpoint) *MultiFrontend {
	if len(frontends) == 0 {
		panic("relay: NewMultiFrontend requires at least one frontend")
	}
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
// for m.ConversationID. If none has (this process lifetime), it asks each
// frontend that implements Claimer whether the id is plausibly theirs (e.g.
// Discord snowflake magnitude vs. a Telegram chat id) before falling back to
// frontends[0], so a relayd-originated message (scheduler reminder, admin
// notice) doesn't get silently misrouted to the wrong platform and dropped —
// see Claimer's doc comment.
func (mf *MultiFrontend) Send(ctx context.Context, m Message) error {
	mf.mu.RLock()
	fe, ok := mf.owner[m.ConversationID]
	mf.mu.RUnlock()
	if !ok {
		for _, cand := range mf.frontends {
			if claimer, isClaimer := cand.(Claimer); isClaimer && claimer.OwnsConversationID(m.ConversationID) {
				fe = cand
				ok = true
				break
			}
		}
	}
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
