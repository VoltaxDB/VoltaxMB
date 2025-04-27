package queue

import (
	"container/heap"
	"fmt"
	"sync"
	"time"

	"github.com/google/uuid"
)

type PriorityMessage struct {
	Message
	Priority int
	Index    int // The index of the item in the heap
}

type PriorityQueue []*PriorityMessage

func (pq PriorityQueue) Len() int { return len(pq) }

func (pq PriorityQueue) Less(i, j int) bool {
	return pq[i].Priority > pq[j].Priority
}

func (pq PriorityQueue) Swap(i, j int) {
	pq[i], pq[j] = pq[j], pq[i]
	pq[i].Index = i
	pq[j].Index = j
}

func (pq *PriorityQueue) Push(x interface{}) {
	n := len(*pq)
	item := x.(*PriorityMessage)
	item.Index = n
	*pq = append(*pq, item)
}

func (pq *PriorityQueue) Pop() interface{} {
	old := *pq
	n := len(old)
	item := old[n-1]
	old[n-1] = nil  // avoid memory leak
	item.Index = -1 // for safety
	*pq = old[0 : n-1]
	return item
}

type PriorityPartition struct {
	ID       int
	Messages *PriorityQueue
	mu       sync.RWMutex
}

func NewPriorityPartition(id int) *PriorityPartition {
	pq := make(PriorityQueue, 0)
	heap.Init(&pq)
	return &PriorityPartition{
		ID:       id,
		Messages: &pq,
	}
}

func (p *PriorityPartition) PushMessage(msg *PriorityMessage) {
	p.mu.Lock()
	defer p.mu.Unlock()
	heap.Push(p.Messages, msg)
}

func (p *PriorityPartition) PopMessage() *PriorityMessage {
	p.mu.Lock()
	defer p.mu.Unlock()
	if p.Messages.Len() == 0 {
		return nil
	}
	return heap.Pop(p.Messages).(*PriorityMessage)
}

type PriorityTopic struct {
	Name       string
	Partitions map[int]*PriorityPartition
	mu         sync.RWMutex
}

func NewPriorityTopic(name string, numPartitions int) *PriorityTopic {
	topic := &PriorityTopic{
		Name:       name,
		Partitions: make(map[int]*PriorityPartition),
	}

	for i := 0; i < numPartitions; i++ {
		topic.Partitions[i] = NewPriorityPartition(i)
	}

	return topic
}

func (t *PriorityTopic) PublishMessage(partitionID int, data []byte, priority int) error {
	t.mu.RLock()
	partition, exists := t.Partitions[partitionID]
	t.mu.RUnlock()

	if !exists {
		return fmt.Errorf("partition %d does not exist", partitionID)
	}

	msg := &PriorityMessage{
		Message: Message{
			ID:        uuid.New().String(),
			Topic:     t.Name,
			Partition: partitionID,
			Data:      data,
			Timestamp: time.Now(),
		},
		Priority: priority,
	}

	partition.PushMessage(msg)
	return nil
}

func (t *PriorityTopic) ConsumeMessage(partitionID int) (*Message, error) {
	t.mu.RLock()
	partition, exists := t.Partitions[partitionID]
	t.mu.RUnlock()

	if !exists {
		return nil, fmt.Errorf("partition %d does not exist", partitionID)
	}

	priorityMsg := partition.PopMessage()
	if priorityMsg == nil {
		return nil, nil
	}

	return &priorityMsg.Message, nil
}
