package queue

import (
	"container/heap"
	"fmt"
	"sync"
	"time"

	"github.com/google/uuid"
)

// ScheduledMessage represents a message scheduled for future delivery
type ScheduledMessage struct {
	Message
	DeliveryTime time.Time
	Index        int // The index of the item in the heap
}

// ScheduledQueue implements heap.Interface
type ScheduledQueue []*ScheduledMessage

func (sq ScheduledQueue) Len() int { return len(sq) }

func (sq ScheduledQueue) Less(i, j int) bool {
	return sq[i].DeliveryTime.Before(sq[j].DeliveryTime)
}

func (sq ScheduledQueue) Swap(i, j int) {
	sq[i], sq[j] = sq[j], sq[i]
	sq[i].Index = i
	sq[j].Index = j
}

func (sq *ScheduledQueue) Push(x interface{}) {
	n := len(*sq)
	item := x.(*ScheduledMessage)
	item.Index = n
	*sq = append(*sq, item)
}

func (sq *ScheduledQueue) Pop() interface{} {
	old := *sq
	n := len(old)
	item := old[n-1]
	old[n-1] = nil  // avoid memory leak
	item.Index = -1 // for safety
	*sq = old[0 : n-1]
	return item
}

type Scheduler struct {
	queue      *ScheduledQueue
	queueMgr   *Manager
	stopCh     chan struct{}
	mu         sync.RWMutex
	deliveryCh chan *ScheduledMessage
}

func NewScheduler(queueMgr *Manager) *Scheduler {
	sq := make(ScheduledQueue, 0)
	heap.Init(&sq)
	return &Scheduler{
		queue:      &sq,
		queueMgr:   queueMgr,
		stopCh:     make(chan struct{}),
		deliveryCh: make(chan *ScheduledMessage, 1000),
	}
}

func (s *Scheduler) Start() {
	go s.run()
}

func (s *Scheduler) Stop() {
	close(s.stopCh)
}

func (s *Scheduler) ScheduleMessage(topic string, partition int, data []byte, delay time.Duration) error {
	s.mu.Lock()
	defer s.mu.Unlock()

	msg := &ScheduledMessage{
		Message: Message{
			ID:        uuid.New().String(),
			Topic:     topic,
			Partition: partition,
			Data:      data,
			Timestamp: time.Now(),
		},
		DeliveryTime: time.Now().Add(delay),
	}

	heap.Push(s.queue, msg)
	return nil
}

func (s *Scheduler) run() {
	ticker := time.NewTicker(time.Second)
	defer ticker.Stop()

	for {
		select {
		case <-ticker.C:
			s.checkScheduledMessages()
		case <-s.stopCh:
			return
		}
	}
}

func (s *Scheduler) checkScheduledMessages() {
	s.mu.Lock()
	defer s.mu.Unlock()

	now := time.Now()
	for s.queue.Len() > 0 {
		msg := (*s.queue)[0]
		if msg.DeliveryTime.After(now) {
			break
		}

		heap.Pop(s.queue)
		s.deliveryCh <- msg
	}
}

func (s *Scheduler) GetDeliveryChannel() <-chan *ScheduledMessage {
	return s.deliveryCh
}

func (s *Scheduler) ProcessDeliveredMessages() {
	for msg := range s.deliveryCh {
		if err := s.queueMgr.PublishMessage(msg.Topic, msg.Partition, msg.Data, 0); err != nil {
			fmt.Printf("Error publishing scheduled message: %v\n", err)
		}
	}
}
