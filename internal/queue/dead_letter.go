package queue

import (
	"fmt"
	"sync"
	"time"
)

type DeadLetterMessage struct {
	Message
	Error       string
	RetryCount  int
	LastAttempt time.Time
}

type DeadLetterQueue struct {
	messages map[string]*DeadLetterMessage // message ID -> message
	mu       sync.RWMutex
	queueMgr *Manager
}

func NewDeadLetterQueue(queueMgr *Manager) *DeadLetterQueue {
	return &DeadLetterQueue{
		messages: make(map[string]*DeadLetterMessage),
		queueMgr: queueMgr,
	}
}

func (q *DeadLetterQueue) AddMessage(msg *Message, err error) error {
	q.mu.Lock()
	defer q.mu.Unlock()

	dlMsg := &DeadLetterMessage{
		Message:     *msg,
		Error:       err.Error(),
		RetryCount:  0,
		LastAttempt: time.Now(),
	}

	q.messages[msg.ID] = dlMsg
	return nil
}

func (q *DeadLetterQueue) RetryMessage(messageID string) error {
	q.mu.Lock()
	defer q.mu.Unlock()

	dlMsg, exists := q.messages[messageID]
	if !exists {
		return fmt.Errorf("message %s not found in dead-letter queue", messageID)
	}

	dlMsg.RetryCount++
	dlMsg.LastAttempt = time.Now()

	if err := q.queueMgr.PublishMessage(dlMsg.Topic, dlMsg.Partition, dlMsg.Data, 0); err != nil {
		return fmt.Errorf("failed to republish message: %v", err)
	}

	delete(q.messages, messageID)
	return nil
}

func (q *DeadLetterQueue) GetMessage(messageID string) (*DeadLetterMessage, error) {
	q.mu.RLock()
	defer q.mu.RUnlock()

	dlMsg, exists := q.messages[messageID]
	if !exists {
		return nil, fmt.Errorf("message %s not found in dead-letter queue", messageID)
	}

	return dlMsg, nil
}

func (q *DeadLetterQueue) ListMessages() []*DeadLetterMessage {
	q.mu.RLock()
	defer q.mu.RUnlock()

	messages := make([]*DeadLetterMessage, 0, len(q.messages))
	for _, msg := range q.messages {
		messages = append(messages, msg)
	}
	return messages
}

func (q *DeadLetterQueue) RemoveMessage(messageID string) error {
	q.mu.Lock()
	defer q.mu.Unlock()

	if _, exists := q.messages[messageID]; !exists {
		return fmt.Errorf("message %s not found in dead-letter queue", messageID)
	}

	delete(q.messages, messageID)
	return nil
}

func (q *DeadLetterQueue) GetRetryCount(messageID string) (int, error) {
	q.mu.RLock()
	defer q.mu.RUnlock()

	dlMsg, exists := q.messages[messageID]
	if !exists {
		return 0, fmt.Errorf("message %s not found in dead-letter queue", messageID)
	}

	return dlMsg.RetryCount, nil
}

func (q *DeadLetterQueue) GetLastAttempt(messageID string) (time.Time, error) {
	q.mu.RLock()
	defer q.mu.RUnlock()

	dlMsg, exists := q.messages[messageID]
	if !exists {
		return time.Time{}, fmt.Errorf("message %s not found in dead-letter queue", messageID)
	}

	return dlMsg.LastAttempt, nil
}
