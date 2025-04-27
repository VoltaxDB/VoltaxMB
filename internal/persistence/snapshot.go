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

// SnapshotManager handles periodic state snapshots
type SnapshotManager struct {
	dataDir  string
	interval time.Duration
	queueMgr *queue.Manager
	aof      *AOFWriter
	mu       sync.Mutex
	stopCh   chan struct{}
}

// NewSnapshotManager creates a new snapshot manager
func NewSnapshotManager(dataDir string, interval time.Duration, queueMgr *queue.Manager, aof *AOFWriter) *SnapshotManager {
	return &SnapshotManager{
		dataDir:  dataDir,
		interval: interval,
		queueMgr: queueMgr,
		aof:      aof,
		stopCh:   make(chan struct{}),
	}
}

// Start starts the snapshot manager
func (m *SnapshotManager) Start() {
	go m.run()
}

// Stop stops the snapshot manager
func (m *SnapshotManager) Stop() {
	close(m.stopCh)
}

// run runs the snapshot manager loop
func (m *SnapshotManager) run() {
	ticker := time.NewTicker(m.interval)
	defer ticker.Stop()

	for {
		select {
		case <-ticker.C:
			if err := m.takeSnapshot(); err != nil {
				fmt.Printf("Error taking snapshot: %v\n", err)
			}
		case <-m.stopCh:
			return
		}
	}
}

// takeSnapshot takes a snapshot of the current state
func (m *SnapshotManager) takeSnapshot() error {
	m.mu.Lock()
	defer m.mu.Unlock()

	// Create snapshot directory if it doesn't exist
	snapshotDir := filepath.Join(m.dataDir, "snapshots")
	if err := os.MkdirAll(snapshotDir, 0755); err != nil {
		return fmt.Errorf("failed to create snapshot directory: %v", err)
	}

	// Create snapshot file
	fileName := filepath.Join(snapshotDir, fmt.Sprintf("snapshot-%s.json", time.Now().Format("2006-01-02-15-04-05")))
	file, err := os.Create(fileName)
	if err != nil {
		return fmt.Errorf("failed to create snapshot file: %v", err)
	}
	defer file.Close()

	// Get current state
	state := struct {
		Timestamp time.Time               `json:"timestamp"`
		Topics    map[string]*queue.Topic `json:"topics"`
	}{
		Timestamp: time.Now(),
		Topics:    m.queueMgr.GetTopics(),
	}

	// Write state to file
	encoder := json.NewEncoder(file)
	encoder.SetIndent("", "  ")
	if err := encoder.Encode(state); err != nil {
		return fmt.Errorf("failed to write snapshot: %v", err)
	}

	// Rotate AOF file after successful snapshot
	if err := m.aof.Rotate(); err != nil {
		return fmt.Errorf("failed to rotate AOF file: %v", err)
	}

	return nil
}

// LoadSnapshot loads the latest snapshot
func (m *SnapshotManager) LoadSnapshot() error {
	m.mu.Lock()
	defer m.mu.Unlock()

	// Find latest snapshot
	snapshotDir := filepath.Join(m.dataDir, "snapshots")
	entries, err := os.ReadDir(snapshotDir)
	if err != nil {
		return fmt.Errorf("failed to read snapshot directory: %v", err)
	}

	var latestSnapshot string
	var latestTime time.Time

	for _, entry := range entries {
		if entry.IsDir() {
			continue
		}

		info, err := entry.Info()
		if err != nil {
			continue
		}

		if info.ModTime().After(latestTime) {
			latestTime = info.ModTime()
			latestSnapshot = filepath.Join(snapshotDir, entry.Name())
		}
	}

	if latestSnapshot == "" {
		return nil // No snapshots found
	}

	// Load snapshot
	file, err := os.Open(latestSnapshot)
	if err != nil {
		return fmt.Errorf("failed to open snapshot file: %v", err)
	}
	defer file.Close()

	var state struct {
		Timestamp time.Time               `json:"timestamp"`
		Topics    map[string]*queue.Topic `json:"topics"`
	}

	if err := json.NewDecoder(file).Decode(&state); err != nil {
		return fmt.Errorf("failed to decode snapshot: %v", err)
	}

	// Restore state
	m.queueMgr.RestoreFromSnapshot(state.Topics)

	return nil
}
