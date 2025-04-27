package consumer

import (
	"fmt"
	"sync"
	"time"

	"github.com/voltaxmq/voltaxmq/internal/queue"
)

type Group struct {
	Name     string
	Members  map[string]*Member
	Offsets  map[string]map[int]int64 // topic -> partition -> offset
	mu       sync.RWMutex
	queueMgr *queue.Manager
}

type Member struct {
	ID         string
	Topics     []string
	Partitions map[string][]int // topic -> partitions
	LastSeen   time.Time
}

type Manager struct {
	groups   map[string]*Group
	queueMgr *queue.Manager
	mu       sync.RWMutex
}

func NewManager(queueMgr *queue.Manager) *Manager {
	return &Manager{
		groups:   make(map[string]*Group),
		queueMgr: queueMgr,
	}
}

func (m *Manager) CreateGroup(name string) error {
	m.mu.Lock()
	defer m.mu.Unlock()

	if _, exists := m.groups[name]; exists {
		return fmt.Errorf("group %s already exists", name)
	}

	m.groups[name] = &Group{
		Name:     name,
		Members:  make(map[string]*Member),
		Offsets:  make(map[string]map[int]int64),
		queueMgr: m.queueMgr,
	}

	return nil
}

func (m *Manager) JoinGroup(groupName, memberID string, topics []string) error {
	m.mu.Lock()
	defer m.mu.Unlock()

	group, exists := m.groups[groupName]
	if !exists {
		return fmt.Errorf("group %s does not exist", groupName)
	}

	group.mu.Lock()
	defer group.mu.Unlock()

	member := &Member{
		ID:         memberID,
		Topics:     topics,
		Partitions: make(map[string][]int),
		LastSeen:   time.Now(),
	}

	for _, topic := range topics {
		partitions := make([]int, 0)
		// TODO: Implement partition assignment strategy
		member.Partitions[topic] = partitions
	}

	group.Members[memberID] = member
	return nil
}

func (m *Manager) LeaveGroup(groupName, memberID string) error {
	m.mu.Lock()
	defer m.mu.Unlock()

	group, exists := m.groups[groupName]
	if !exists {
		return fmt.Errorf("group %s does not exist", groupName)
	}

	group.mu.Lock()
	defer group.mu.Unlock()

	delete(group.Members, memberID)
	return nil
}

func (m *Manager) UpdateOffset(groupName, topic string, partition int, offset int64) error {
	m.mu.RLock()
	group, exists := m.groups[groupName]
	m.mu.RUnlock()

	if !exists {
		return fmt.Errorf("group %s does not exist", groupName)
	}

	group.mu.Lock()
	defer group.mu.Unlock()

	if _, exists := group.Offsets[topic]; !exists {
		group.Offsets[topic] = make(map[int]int64)
	}

	group.Offsets[topic][partition] = offset
	return nil
}

func (m *Manager) GetOffset(groupName, topic string, partition int) (int64, error) {
	m.mu.RLock()
	group, exists := m.groups[groupName]
	m.mu.RUnlock()

	if !exists {
		return 0, fmt.Errorf("group %s does not exist", groupName)
	}

	group.mu.RLock()
	defer group.mu.RUnlock()

	if topicOffsets, exists := group.Offsets[topic]; exists {
		if offset, exists := topicOffsets[partition]; exists {
			return offset, nil
		}
	}

	return 0, nil
}

func (m *Manager) GetGroup(name string) (*Group, error) {
	m.mu.RLock()
	defer m.mu.RUnlock()

	group, exists := m.groups[name]
	if !exists {
		return nil, fmt.Errorf("group %s does not exist", name)
	}

	return group, nil
}

func (m *Manager) ListGroups() []string {
	m.mu.RLock()
	defer m.mu.RUnlock()

	groups := make([]string, 0, len(m.groups))
	for name := range m.groups {
		groups = append(groups, name)
	}
	return groups
}
