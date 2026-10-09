// Package msgstore is the relay's full-content message pipeline: unlike the
// eventlog (which deliberately records only metadata + byte counts), this
// captures the message BODY of every relay event and publishes it to Kafka,
// where a downstream consumer writes it to Postgres as the durable, ordered
// system-of-record of every interaction.
//
// Design goals, in order:
//  1. Never lose a message. If Kafka is unreachable, records spool to a local
//     append-only file and a background loop replays them when Kafka recovers.
//  2. Never break message flow. A nil *Producer is valid and discards, and
//     Publish never returns an error — persistence must not take the relay down,
//     exactly like eventlog.
//  3. Capture everything, including denials. The single relay chokepoint that
//     feeds this also fires on gate-blocked messages, so probes that never reach
//     the model are recorded too.
package msgstore

import (
	"context"
	"crypto/rand"
	"encoding/json"
	"os"
	"sync"
	"time"

	"github.com/oklog/ulid/v2"
	"github.com/segmentio/kafka-go"
)

// Record is the full-content event published to Kafka, mirrored 1:1 by the
// Postgres `messages` table (minus seq, which Postgres assigns on insert to give
// a gap-free total order).
type Record struct {
	MsgID     string            `json:"msg_id"`
	TS        string            `json:"ts"`
	Event     string            `json:"event"`
	Direction string            `json:"direction,omitempty"`
	Kind      string            `json:"kind,omitempty"`
	Frontend  string            `json:"frontend,omitempty"`
	ChatID    string            `json:"chat_id,omitempty"`
	FromID    string            `json:"from_id,omitempty"`
	FromName  string            `json:"from_name,omitempty"`
	InReplyTo string            `json:"in_reply_to,omitempty"`
	Content   string            `json:"content,omitempty"`
	Detail    string            `json:"detail,omitempty"`
	Err       string            `json:"err,omitempty"`
	Meta      map[string]string `json:"meta,omitempty"`
}

// Producer publishes Records to Kafka with a durable local spool fallback.
type Producer struct {
	w      *kafka.Writer
	topic  string
	spool  string
	mu     sync.Mutex // guards spool file writes
	wake   chan struct{}
	closed chan struct{}
	once   sync.Once
}

// Config configures a Producer.
type Config struct {
	Brokers []string // e.g. []string{"localhost:9094"}
	Topic   string   // e.g. "relay-messages"
	Spool   string   // path to the local fallback file, e.g. "/var/lib/agent-relay/relay-messages.spool"
}

// New builds a Producer. Returns a nil *Producer (valid, discards) if Brokers or
// Topic are empty, so callers never need a nil check and persistence is simply
// off when unconfigured.
func New(cfg Config) *Producer {
	if len(cfg.Brokers) == 0 || cfg.Topic == "" {
		return nil
	}
	p := &Producer{
		w: &kafka.Writer{
			Addr:         kafka.TCP(cfg.Brokers...),
			Topic:        cfg.Topic,
			Balancer:     &kafka.LeastBytes{},
			BatchTimeout: 200 * time.Millisecond,
			RequiredAcks: kafka.RequireAll, // don't consider a write done until it's durable
			Async:        false,
		},
		topic:  cfg.Topic,
		spool:  cfg.Spool,
		wake:   make(chan struct{}, 1),
		closed: make(chan struct{}),
	}
	if p.spool != "" {
		go p.replayLoop()
	}
	return p
}

// newMsgID mints a time-sortable ULID for records that arrive without one
// (deterministic/scheduled sends). Ordering is authoritative via Postgres seq;
// the ULID just guarantees a unique, roughly-sortable id per Jean's spec.
func newMsgID() string {
	return ulid.MustNew(ulid.Timestamp(time.Now()), rand.Reader).String()
}

// Publish records one event. Never blocks the caller for long and never errors:
// on any Kafka failure the record is spooled to disk for later replay. A nil
// Producer discards.
func (p *Producer) Publish(r Record) {
	if p == nil {
		return
	}
	if r.MsgID == "" {
		r.MsgID = newMsgID()
	}
	if r.TS == "" {
		r.TS = time.Now().UTC().Format(time.RFC3339Nano)
	}
	b, err := json.Marshal(r)
	if err != nil {
		return // an unmarshalable record is not worth taking the relay down
	}
	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
	defer cancel()
	if err := p.w.WriteMessages(ctx, kafka.Message{Key: []byte(r.ChatID), Value: b}); err != nil {
		p.spoolAppend(b) // Kafka down/slow -> never lose it, land it on disk
	}
}

// spoolAppend writes one JSON line to the local fallback file and nudges the
// replay loop. Best-effort: if even the disk write fails there is nothing left
// to do but keep the relay running.
func (p *Producer) spoolAppend(line []byte) {
	if p.spool == "" {
		return
	}
	p.mu.Lock()
	f, err := os.OpenFile(p.spool, os.O_CREATE|os.O_WRONLY|os.O_APPEND, 0o600)
	if err == nil {
		_, _ = f.Write(append(line, '\n'))
		_ = f.Close()
	}
	p.mu.Unlock()
	select {
	case p.wake <- struct{}{}:
	default:
	}
}

// replayLoop drains the spool file back into Kafka whenever there is spooled
// data and Kafka accepts it. It rewrites the spool with only the records that
// still failed, so a partial drain makes progress and never double-drops.
func (p *Producer) replayLoop() {
	ticker := time.NewTicker(30 * time.Second)
	defer ticker.Stop()
	for {
		select {
		case <-p.closed:
			return
		case <-ticker.C:
		case <-p.wake:
		}
		p.drainSpool()
	}
}

func (p *Producer) drainSpool() {
	p.mu.Lock()
	data, err := os.ReadFile(p.spool)
	if err != nil || len(data) == 0 {
		p.mu.Unlock()
		return
	}
	// Take ownership of the current spool contents, truncating so new failures
	// during replay append cleanly after us.
	_ = os.Truncate(p.spool, 0)
	p.mu.Unlock()

	var remaining [][]byte
	for _, line := range splitLines(data) {
		if len(line) == 0 {
			continue
		}
		ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
		err := p.w.WriteMessages(ctx, kafka.Message{Value: line})
		cancel()
		if err != nil {
			remaining = append(remaining, line)
		}
	}
	if len(remaining) > 0 {
		p.mu.Lock()
		f, err := os.OpenFile(p.spool, os.O_CREATE|os.O_WRONLY|os.O_APPEND, 0o600)
		if err == nil {
			for _, line := range remaining {
				_, _ = f.Write(append(line, '\n'))
			}
			_ = f.Close()
		}
		p.mu.Unlock()
	}
}

func splitLines(b []byte) [][]byte {
	var out [][]byte
	start := 0
	for i, c := range b {
		if c == '\n' {
			out = append(out, b[start:i])
			start = i + 1
		}
	}
	if start < len(b) {
		out = append(out, b[start:])
	}
	return out
}

// Close stops the replay loop and closes the Kafka writer.
func (p *Producer) Close() error {
	if p == nil {
		return nil
	}
	p.once.Do(func() { close(p.closed) })
	return p.w.Close()
}
