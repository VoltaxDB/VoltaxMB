package persistence

import (
	"encoding/json"
	"fmt"
	"os"
	"path/filepath"
	"sync"
	"time"

	"github.com/voltaxmq/voltaxmq/internal/queue"
)

// AOFWriter handles append-only file writing
type AOFWriter struct {
	file     *os.File
	mu       sync.Mutex
	dataDir  string
	fileName string
}

// NewAOFWriter creates a new AOF writer
func NewAOFWriter(dataDir string) (*AOFWriter, error) {
	if err := os.MkdirAll(dataDir, 0755); err != nil {
		return nil, fmt.Errorf("failed to create data directory: %v", err)
	}

	fileName := filepath.Join(dataDir, fmt.Sprintf("aof-%s.log", time.Now().Format("2006-01-02-15-04-05")))
	file, err := os.OpenFile(fileName, os.O_APPEND|os.O_CREATE|os.O_WRONLY, 0644)
	if err != nil {
		return nil, fmt.Errorf("failed to open AOF file: %v", err)
	}

	return &AOFWriter{
		file:     file,
		dataDir:  dataDir,
		fileName: fileName,
	}, nil
}

// WriteMessage writes a message to the AOF file
func (w *AOFWriter) WriteMessage(msg *queue.Message) error {
	w.mu.Lock()
	defer w.mu.Unlock()

	entry := struct {
		Type      string         `json:"type"`
		Timestamp time.Time      `json:"timestamp"`
		Message   *queue.Message `json:"message"`
	}{
		Type:      "message",
		Timestamp: time.Now(),
		Message:   msg,
	}

	data, err := json.Marshal(entry)
	if err != nil {
		return fmt.Errorf("failed to marshal message: %v", err)
	}

	if _, err := w.file.Write(append(data, '\n')); err != nil {
		return fmt.Errorf("failed to write to AOF file: %v", err)
	}

	return w.file.Sync()
}

// WriteTopicCreation writes a topic creation event to the AOF file
func (w *AOFWriter) WriteTopicCreation(name string, numPartitions int) error {
	w.mu.Lock()
	defer w.mu.Unlock()

	entry := struct {
		Type          string    `json:"type"`
		Timestamp     time.Time `json:"timestamp"`
		Name          string    `json:"name"`
		NumPartitions int       `json:"num_partitions"`
	}{
		Type:          "topic_creation",
		Timestamp:     time.Now(),
		Name:          name,
		NumPartitions: numPartitions,
	}

	data, err := json.Marshal(entry)
	if err != nil {
		return fmt.Errorf("failed to marshal topic creation: %v", err)
	}

	if _, err := w.file.Write(append(data, '\n')); err != nil {
		return fmt.Errorf("failed to write to AOF file: %v", err)
	}

	return w.file.Sync()
}

// Close closes the AOF writer
func (w *AOFWriter) Close() error {
	w.mu.Lock()
	defer w.mu.Unlock()

	if err := w.file.Sync(); err != nil {
		return fmt.Errorf("failed to sync AOF file: %v", err)
	}

	return w.file.Close()
}

// Rotate rotates the AOF file
func (w *AOFWriter) Rotate() error {
	w.mu.Lock()
	defer w.mu.Unlock()

	// Close current file
	if err := w.file.Close(); err != nil {
		return fmt.Errorf("failed to close current AOF file: %v", err)
	}

	// Create new file
	fileName := filepath.Join(w.dataDir, fmt.Sprintf("aof-%s.log", time.Now().Format("2006-01-02-15-04-05")))
	file, err := os.OpenFile(fileName, os.O_APPEND|os.O_CREATE|os.O_WRONLY, 0644)
	if err != nil {
		return fmt.Errorf("failed to open new AOF file: %v", err)
	}

	w.file = file
	w.fileName = fileName
	return nil
}
