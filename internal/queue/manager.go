package queue

import (
	"fmt"
	"sort"
	"sync"
	"time"

	"github.com/google/uuid"
	"github.com/voltaxmq/voltaxmq/internal/metrics"
)

type Message struct {
	ID        string
	Topic     string
	Partition int
	Data      []byte
	Timestamp time.Time
	Priority  int
}

type Partition struct {
	ID       int
	Messages []Message
	mu       sync.RWMutex
}

type Topic struct {
	Name          string
	Partitions    map[int]*Partition
	mu            sync.RWMutex
	numPartitions int
	partitions    []*Partition
}

type ConsumerGroup struct {
	Name    string
	Topics  []string
	Members map[string]string // memberID -> clientID
	mu      sync.RWMutex
}

type Manager struct {
	topics            map[string]*Topic
	groups            map[string]*ConsumerGroup
	mu                sync.RWMutex
	metrics           *metrics.Metrics
	scheduledMessages []*Message
}

func NewManager() *Manager {
	return &Manager{
		topics: make(map[string]*Topic),
		groups: make(map[string]*ConsumerGroup),
	}
}

func (m *Manager) SetMetrics(metrics *metrics.Metrics) {
	m.metrics = metrics
}

func (m *Manager) GetTopics() map[string]*Topic {
	m.mu.RLock()
	defer m.mu.RUnlock()

	topics := make(map[string]*Topic)
	for name, topic := range m.topics {
		topics[name] = topic
	}
	return topics
}

func (m *Manager) RestoreFromSnapshot(topics map[string]*Topic) {
	m.mu.Lock()
	defer m.mu.Unlock()

	m.topics = topics
}

func (m *Manager) CreateTopic(name string, numPartitions int) error {
	m.mu.Lock()
	defer m.mu.Unlock()

	if _, exists := m.topics[name]; exists {
		return fmt.Errorf("topic %s already exists", name)
	}

	topic := &Topic{
		Name:          name,
		Partitions:    make(map[int]*Partition),
		numPartitions: numPartitions,
		partitions:    make([]*Partition, numPartitions),
	}

	for i := 0; i < numPartitions; i++ {
		topic.Partitions[i] = &Partition{
			ID:       i,
			Messages: make([]Message, 0),
		}
		topic.partitions[i] = topic.Partitions[i]
	}

	m.topics[name] = topic

	if m.metrics != nil {
		m.metrics.RecordTopicCreated(numPartitions)
	}

	return nil
}

func (m *Manager) PublishMessage(topicName string, partitionID int, data []byte, priority int) error {
	m.mu.RLock()
	topic, exists := m.topics[topicName]
	m.mu.RUnlock()

	if !exists {
		return fmt.Errorf("topic %s does not exist", topicName)
	}

	topic.mu.Lock()
	defer topic.mu.Unlock()

	partition, exists := topic.Partitions[partitionID]
	if !exists {
		return fmt.Errorf("partition %d does not exist in topic %s", partitionID, topicName)
	}

	message := Message{
		ID:        uuid.New().String(),
		Topic:     topicName,
		Partition: partitionID,
		Data:      data,
		Timestamp: time.Now(),
		Priority:  priority,
	}

	partition.mu.Lock()
	partition.Messages = append(partition.Messages, message)
	partition.mu.Unlock()

	if m.metrics != nil {
		m.metrics.RecordMessagePublished(len(data))
	}

	return nil
}

func (m *Manager) ConsumeMessage(topicName string, partitionID int) (*Message, error) {
	m.mu.RLock()
	topic, exists := m.topics[topicName]
	m.mu.RUnlock()

	if !exists {
		return nil, fmt.Errorf("topic %s does not exist", topicName)
	}

	topic.mu.RLock()
	partition, exists := topic.Partitions[partitionID]
	topic.mu.RUnlock()

	if !exists {
		return nil, fmt.Errorf("partition %d does not exist in topic %s", partitionID, topicName)
	}

	partition.mu.Lock()
	defer partition.mu.Unlock()

	if len(partition.Messages) == 0 {
		return nil, nil
	}

	message := partition.Messages[0]
	partition.Messages = partition.Messages[1:]

	if m.metrics != nil {
		m.metrics.RecordMessageConsumed(0) // TODO: Add latency tracking
	}

	return &message, nil
}

func (m *Manager) GetTopic(name string) (*Topic, error) {
	m.mu.RLock()
	defer m.mu.RUnlock()

	topic, exists := m.topics[name]
	if !exists {
		return nil, fmt.Errorf("topic %s does not exist", name)
	}

	return topic, nil
}

func (m *Manager) ListTopics() []string {
	m.mu.RLock()
	defer m.mu.RUnlock()

	topics := make([]string, 0, len(m.topics))
	for name := range m.topics {
		topics = append(topics, name)
	}
	return topics
}

func (m *Manager) DeleteTopic(name string) error {
	m.mu.Lock()
	defer m.mu.Unlock()

	if _, exists := m.topics[name]; !exists {
		return fmt.Errorf("topic %s does not exist", name)
	}

	delete(m.topics, name)

	if m.metrics != nil {
		m.metrics.RecordTopicDeleted()
	}

	return nil
}

func (m *Manager) AcknowledgeMessage(topicName string, partitionID int, messageID string) error {
	m.mu.RLock()
	topic, exists := m.topics[topicName]
	m.mu.RUnlock()

	if !exists {
		return fmt.Errorf("topic %s does not exist", topicName)
	}

	topic.mu.RLock()
	partition, exists := topic.Partitions[partitionID]
	topic.mu.RUnlock()

	if !exists {
		return fmt.Errorf("partition %d does not exist in topic %s", partitionID, topicName)
	}

	partition.mu.Lock()
	defer partition.mu.Unlock()

	// Find and remove the message with the given ID
	for i, msg := range partition.Messages {
		if msg.ID == messageID {
			// Remove the message from the partition
			partition.Messages = append(partition.Messages[:i], partition.Messages[i+1:]...)
			return nil
		}
	}

	return fmt.Errorf("message %s not found in topic %s partition %d", messageID, topicName, partitionID)
}

func (m *Manager) GetDLQMessages(dlqTopic string) ([]Message, error) {
	m.mu.RLock()
	topic, exists := m.topics[dlqTopic]
	m.mu.RUnlock()

	if !exists {
		return nil, fmt.Errorf("DLQ topic %s does not exist", dlqTopic)
	}

	topic.mu.RLock()
	defer topic.mu.RUnlock()

	// DLQ topics only have one partition (partition 0)
	partition, exists := topic.Partitions[0]
	if !exists {
		return nil, fmt.Errorf("partition 0 does not exist in DLQ topic %s", dlqTopic)
	}

	partition.mu.RLock()
	defer partition.mu.RUnlock()

	// Return a copy of the messages
	messages := make([]Message, len(partition.Messages))
	copy(messages, partition.Messages)

	return messages, nil
}

func (m *Manager) CreateConsumerGroup(name string, topics []string) error {
	m.mu.Lock()
	defer m.mu.Unlock()

	if _, exists := m.groups[name]; exists {
		return fmt.Errorf("consumer group %s already exists", name)
	}

	// Verify all topics exist
	for _, topic := range topics {
		if _, exists := m.topics[topic]; !exists {
			return fmt.Errorf("topic %s does not exist", topic)
		}
	}

	m.groups[name] = &ConsumerGroup{
		Name:    name,
		Topics:  topics,
		Members: make(map[string]string),
	}

	return nil
}

func (m *Manager) ListConsumerGroups() []string {
	m.mu.RLock()
	defer m.mu.RUnlock()

	groups := make([]string, 0, len(m.groups))
	for name := range m.groups {
		groups = append(groups, name)
	}
	return groups
}

func (m *Manager) JoinConsumerGroup(groupName, memberID string) error {
	m.mu.RLock()
	group, exists := m.groups[groupName]
	m.mu.RUnlock()

	if !exists {
		return fmt.Errorf("consumer group %s does not exist", groupName)
	}

	group.mu.Lock()
	defer group.mu.Unlock()

	group.Members[memberID] = memberID
	return nil
}

func (m *Manager) LeaveConsumerGroup(groupName, memberID string) error {
	m.mu.RLock()
	group, exists := m.groups[groupName]
	m.mu.RUnlock()

	if !exists {
		return fmt.Errorf("consumer group %s does not exist", groupName)
	}

	group.mu.Lock()
	defer group.mu.Unlock()

	delete(group.Members, memberID)
	return nil
}

// ScheduleMessage schedules a message for future delivery
func (m *Manager) ScheduleMessage(topic string, partition int, data []byte, delay int) error {
	m.mu.Lock()
	defer m.mu.Unlock()

	t, exists := m.topics[topic]
	if !exists {
		return fmt.Errorf("topic %s does not exist", topic)
	}

	if partition < 0 || partition >= t.numPartitions {
		return fmt.Errorf("invalid partition %d for topic %s", partition, topic)
	}

	msg := &Message{
		ID:        uuid.New().String(),
		Topic:     topic,
		Partition: partition,
		Data:      data,
		Priority:  1, // Default priority for scheduled messages
		Timestamp: time.Now().Add(time.Duration(delay) * time.Millisecond),
	}

	// Add to scheduled messages
	m.scheduledMessages = append(m.scheduledMessages, msg)

	// Sort scheduled messages by timestamp
	sort.Slice(m.scheduledMessages, func(i, j int) bool {
		return m.scheduledMessages[i].Timestamp.Before(m.scheduledMessages[j].Timestamp)
	})

	return nil
}

// MoveToDLQ moves a message to the Dead Letter Queue
func (m *Manager) MoveToDLQ(topic string, partition int, data []byte, reason string) error {
	m.mu.Lock()
	defer m.mu.Unlock()

	// Create DLQ topic if it doesn't exist
	dlqTopic := topic + "-dlq"
	if _, exists := m.topics[dlqTopic]; !exists {
		if err := m.CreateTopic(dlqTopic, 1); err != nil {
			return fmt.Errorf("failed to create DLQ topic: %v", err)
		}
	}

	// Get DLQ topic
	dlq, exists := m.topics[dlqTopic]
	if !exists {
		return fmt.Errorf("DLQ topic %s does not exist", dlqTopic)
	}

	// Get DLQ partition (always partition 0)
	dlqPartition, exists := dlq.Partitions[0]
	if !exists {
		return fmt.Errorf("partition 0 does not exist in DLQ topic %s", dlqTopic)
	}

	// Create DLQ message
	msg := Message{
		ID:        uuid.New().String(),
		Topic:     dlqTopic,
		Partition: 0,
		Data:      data,
		Timestamp: time.Now(),
		Priority:  1, // Default priority for DLQ messages
	}

	// Add message to DLQ
	dlqPartition.mu.Lock()
	dlqPartition.Messages = append(dlqPartition.Messages, msg)
	dlqPartition.mu.Unlock()

	if m.metrics != nil {
		m.metrics.RecordMessagePublished(len(data))
	}

	return nil
}
