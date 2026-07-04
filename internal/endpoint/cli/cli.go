// Package cli is a frontend Endpoint that reads user lines from an io.Reader and
// writes replies to an io.Writer. It stands in for a chat platform (Telegram,
// Discord) so the broker, commands, and budget gate can be driven from a
// terminal or a piped script. This is the "command line source" the relay's
// control toggles (/rate, /tier, ...) are sent from.
package cli

import (
	"bufio"
	"context"
	"fmt"
	"io"

	"github.com/jeanhaley32/agent-relay/internal/relay"
)

// Frontend is a line-oriented CLI Endpoint.
type Frontend struct {
	conv string
	out  io.Writer
	recv chan relay.Message
}

// New starts reading lines from in on a background goroutine, emitting each as a
// user Message. The Recv channel closes when in reaches EOF.
func New(conv string, in io.Reader, out io.Writer) *Frontend {
	f := &Frontend{conv: conv, out: out, recv: make(chan relay.Message, 16)}
	go f.readLoop(in)
	return f
}

func (f *Frontend) readLoop(in io.Reader) {
	defer close(f.recv)
	sc := bufio.NewScanner(in)
	for sc.Scan() {
		line := sc.Text()
		if line == "" {
			continue
		}
		f.recv <- relay.UserMsg(f.conv, line)
	}
}

func (f *Frontend) Name() string               { return "cli" }
func (f *Frontend) Recv() <-chan relay.Message { return f.recv }
func (f *Frontend) Close() error               { return nil }

// Send prints an assistant/reply message to the output writer.
func (f *Frontend) Send(_ context.Context, m relay.Message) error {
	_, err := fmt.Fprintf(f.out, "  << %s\n", m.Text)
	return err
}
