// Package router implements a relay.Endpoint that fans multiple sub-frontends
// into one shared relay.Broker. Each sub-frontend carries its own admin layer;
// the Router is the only path a message can take to reach the Broker, and it
// refuses anything its sub-frontend's admin layer hasn't cleared.
package router

import (
	"context"
	"fmt"
	"sync"

	"github.com/jeanhaley32/agent-relay/internal/relay"
)

// FrontendMetaKey is the relay.Message.Meta key the Router stamps on every
// inbound message to record which sub-frontend it came from, and reads to
// route an outbound Send back to the right one.
const FrontendMetaKey = "frontend"

// SubFrontend is a transport registered with the Router. Recv must emit only
// messages that have already been cleared by the frontend's own admin layer
// (enforced by AdminLayer.Clear before a message ever reaches the Router).
type SubFrontend interface {
	Name() string
	Recv() <-chan relay.Message
	Send(ctx context.Context, m relay.Message) error
	Close() error
}

// AdminLayer gates every inbound message for one sub-frontend. Clear returns
// the (possibly rewritten) message and whether it may proceed to the Router.
// A rejected message is dropped and counted; it never reaches Router.Recv().
type AdminLayer interface {
	Clear(m relay.Message) (relay.Message, bool)
}

type registration struct {
	sf    SubFrontend
	admin AdminLayer
}

// Router implements relay.Endpoint and is the only component allowed to hand
// messages to a relay.Broker's Frontend slot.
type Router struct {
	name string

	mu    sync.RWMutex
	subs  map[string]*registration
	stats map[string]int // per-frontend admin-rejection count

	out    chan relay.Message
	cancel func()
	wg     sync.WaitGroup

	closeOnce sync.Once
}

// New creates an empty Router. Register sub-frontends before Recv is consumed.
func New(name string) *Router {
	return &Router{
		name:  name,
		subs:  make(map[string]*registration),
		stats: make(map[string]int),
		out:   make(chan relay.Message, 64),
	}
}

// Name implements relay.Endpoint.
func (r *Router) Name() string { return r.name }

// Register adds a sub-frontend behind its admin layer and starts fanning its
// Recv channel into the Router's own output. AdminLayer must not be nil —
// there is no constructor path to a registered sub-frontend without one.
func (r *Router) Register(sf SubFrontend, admin AdminLayer) error {
	if sf == nil {
		return fmt.Errorf("router: nil sub-frontend")
	}
	if admin == nil {
		return fmt.Errorf("router: nil admin layer for sub-frontend %q", sf.Name())
	}
	name := sf.Name()

	r.mu.Lock()
	if _, exists := r.subs[name]; exists {
		r.mu.Unlock()
		return fmt.Errorf("router: sub-frontend %q already registered", name)
	}
	r.subs[name] = &registration{sf: sf, admin: admin}
	r.mu.Unlock()

	r.wg.Add(1)
	go r.pump(name, sf, admin)
	return nil
}

func (r *Router) pump(name string, sf SubFrontend, admin AdminLayer) {
	defer r.wg.Done()
	for m := range sf.Recv() {
		cleared, ok := admin.Clear(m)
		if !ok {
			r.mu.Lock()
			r.stats[name]++
			r.mu.Unlock()
			continue
		}
		if cleared.Meta == nil {
			cleared.Meta = map[string]string{}
		}
		cleared.Meta[FrontendMetaKey] = name
		r.out <- cleared
	}
}

// Recv implements relay.Endpoint. Every message carries Meta[FrontendMetaKey].
func (r *Router) Recv() <-chan relay.Message { return r.out }

// Send implements relay.Endpoint, routing to the sub-frontend named in
// Meta[FrontendMetaKey]. It returns an error and delivers to nobody if that
// key is missing or names an unregistered sub-frontend — it never guesses.
func (r *Router) Send(ctx context.Context, m relay.Message) error {
	name := m.Meta[FrontendMetaKey]
	if name == "" {
		return fmt.Errorf("router: message missing Meta[%q], cannot route reply", FrontendMetaKey)
	}
	r.mu.RLock()
	reg, ok := r.subs[name]
	r.mu.RUnlock()
	if !ok {
		return fmt.Errorf("router: no registered sub-frontend named %q", name)
	}
	return reg.sf.Send(ctx, m)
}

// Stats returns a snapshot of per-frontend admin-rejection counts.
func (r *Router) Stats() map[string]int {
	r.mu.RLock()
	defer r.mu.RUnlock()
	out := make(map[string]int, len(r.stats))
	for k, v := range r.stats {
		out[k] = v
	}
	return out
}

// Close implements relay.Endpoint: closes every registered sub-frontend, waits
// for their pumps to drain, then closes the Router's own output channel.
func (r *Router) Close() error {
	var err error
	r.closeOnce.Do(func() {
		r.mu.RLock()
		subs := make([]SubFrontend, 0, len(r.subs))
		for _, reg := range r.subs {
			subs = append(subs, reg.sf)
		}
		r.mu.RUnlock()

		for _, sf := range subs {
			if cerr := sf.Close(); cerr != nil && err == nil {
				err = cerr
			}
		}
		r.wg.Wait()
		close(r.out)
	})
	return err
}
