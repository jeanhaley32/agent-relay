package telegram

import (
	"encoding/json"
	"os"
	"sync"
	"time"
)

// FileDeniedLogger appends one JSON line per denied-sender attempt to path,
// creating it (and any parent directories) if needed.
type FileDeniedLogger struct {
	mu   sync.Mutex
	file *os.File
}

type deniedEntry struct {
	At     string `json:"at"`
	ID     int64  `json:"id"`
	Name   string `json:"name"`
	ChatID string `json:"chat_id"`
	Text   string `json:"text"`
}

// NewFileDeniedLogger opens path for append, creating it if it doesn't exist.
func NewFileDeniedLogger(path string) (*FileDeniedLogger, error) {
	f, err := os.OpenFile(path, os.O_APPEND|os.O_CREATE|os.O_WRONLY, 0600)
	if err != nil {
		return nil, err
	}
	return &FileDeniedLogger{file: f}, nil
}

func (d *FileDeniedLogger) LogDenied(id int64, name, chatID, text string) {
	d.mu.Lock()
	defer d.mu.Unlock()
	line, err := json.Marshal(deniedEntry{
		At:     time.Now().UTC().Format(time.RFC3339),
		ID:     id,
		Name:   name,
		ChatID: chatID,
		Text:   text,
	})
	if err != nil {
		return
	}
	line = append(line, '\n')
	_, _ = d.file.Write(line)
}

func (d *FileDeniedLogger) Close() error {
	d.mu.Lock()
	defer d.mu.Unlock()
	return d.file.Close()
}
