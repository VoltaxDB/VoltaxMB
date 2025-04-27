package broker

import (
	"encoding/json"
	"fmt"
	"log"
	"net/http"
	"strconv"
	"strings"
	"time"

	"github.com/google/uuid"
	"github.com/voltaxmq/voltaxmq/internal/metrics"
	"github.com/voltaxmq/voltaxmq/internal/queue"
)

type HTTPServer struct {
	manager   *queue.Manager
	port      int
	metrics   *metrics.Metrics
	scheduler *queue.Scheduler
}

func NewHTTPServer(queueManager *queue.Manager, scheduler *queue.Scheduler, port int) *HTTPServer {
	return &HTTPServer{
		manager:   queueManager,
		port:      port,
		scheduler: scheduler,
	}
}

func (s *HTTPServer) SetMetrics(metrics *metrics.Metrics) {
	s.metrics = metrics
}

// Start starts the HTTP server
func (s *HTTPServer) Start() error {
	http.HandleFunc("/api/topics", s.handleTopics)
	http.HandleFunc("/api/publish", s.handlePublish)
	http.HandleFunc("/api/consume", s.handleConsume)
	http.HandleFunc("/api/ack", s.handleAck)
	http.HandleFunc("/topics/", s.handleTopicOperations)
	http.HandleFunc("/dlq/", s.handleDLQOperations)
	http.HandleFunc("/groups", s.handleConsumerGroups)
	http.HandleFunc("/groups/", s.handleGroupOperations)

	addr := fmt.Sprintf(":%d", s.port)
	log.Printf("Starting HTTP server on %s", addr)
	return http.ListenAndServe(addr, nil)
}

func (s *HTTPServer) handleTopics(w http.ResponseWriter, r *http.Request) {
	if s.metrics != nil {
		s.metrics.ActiveConnections.Inc()
		defer s.metrics.ActiveConnections.Dec()
	}

	switch r.Method {
	case http.MethodPost:
		var req struct {
			Name          string `json:"name"`
			NumPartitions int    `json:"num_partitions"`
		}
		if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
			http.Error(w, "Invalid request body", http.StatusBadRequest)
			return
		}

		if err := s.manager.CreateTopic(req.Name, req.NumPartitions); err != nil {
			http.Error(w, err.Error(), http.StatusInternalServerError)
			return
		}

		w.WriteHeader(http.StatusCreated)
	case http.MethodGet:
		topics := s.manager.ListTopics()
		json.NewEncoder(w).Encode(topics)
	case http.MethodDelete:
		// Extract topic name from query parameter
		topicName := r.URL.Query().Get("name")
		if topicName == "" {
			http.Error(w, "Topic name is required", http.StatusBadRequest)
			return
		}

		if err := s.manager.DeleteTopic(topicName); err != nil {
			http.Error(w, err.Error(), http.StatusInternalServerError)
			return
		}

		w.WriteHeader(http.StatusOK)
	default:
		http.Error(w, "Method not allowed", http.StatusMethodNotAllowed)
	}
}

func (s *HTTPServer) handlePublish(w http.ResponseWriter, r *http.Request) {
	if s.metrics != nil {
		s.metrics.ActiveConnections.Inc()
		defer s.metrics.ActiveConnections.Dec()
	}

	if r.Method != http.MethodPost {
		http.Error(w, "Method not allowed", http.StatusMethodNotAllowed)
		return
	}

	var req struct {
		Topic     string `json:"topic"`
		Partition int    `json:"partition"`
		Data      []byte `json:"data"`
		Priority  int    `json:"priority"`
	}
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
		http.Error(w, "Invalid request body", http.StatusBadRequest)
		return
	}

	if err := s.manager.PublishMessage(req.Topic, req.Partition, req.Data, req.Priority); err != nil {
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}

	w.WriteHeader(http.StatusCreated)
}

func (s *HTTPServer) handleConsume(w http.ResponseWriter, r *http.Request) {
	if s.metrics != nil {
		s.metrics.ActiveConnections.Inc()
		defer s.metrics.ActiveConnections.Dec()
	}

	if r.Method != http.MethodGet {
		http.Error(w, "Method not allowed", http.StatusMethodNotAllowed)
		return
	}

	topic := r.URL.Query().Get("topic")
	partitionStr := r.URL.Query().Get("partition")
	partition, err := strconv.Atoi(partitionStr)
	if err != nil {
		http.Error(w, "Invalid partition", http.StatusBadRequest)
		return
	}

	message, err := s.manager.ConsumeMessage(topic, partition)
	if err != nil {
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}

	if message == nil {
		w.WriteHeader(http.StatusNoContent)
		return
	}

	json.NewEncoder(w).Encode(message)
}

func (s *HTTPServer) handleAck(w http.ResponseWriter, r *http.Request) {
	if s.metrics != nil {
		s.metrics.ActiveConnections.Inc()
		defer s.metrics.ActiveConnections.Dec()
	}

	if r.Method != http.MethodPost {
		http.Error(w, "Method not allowed", http.StatusMethodNotAllowed)
		return
	}

	var req struct {
		MessageID string `json:"message_id"`
		Topic     string `json:"topic"`
		Partition int    `json:"partition"`
	}
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
		http.Error(w, "Invalid request body", http.StatusBadRequest)
		return
	}

	if err := s.manager.AcknowledgeMessage(req.Topic, req.Partition, req.MessageID); err != nil {
		http.Error(w, fmt.Sprintf("Failed to acknowledge message: %v", err), http.StatusInternalServerError)
		return
	}

	w.WriteHeader(http.StatusOK)
}

func (s *HTTPServer) handleTopicOperations(w http.ResponseWriter, r *http.Request) {
	if s.metrics != nil {
		s.metrics.ActiveConnections.Inc()
		defer s.metrics.ActiveConnections.Dec()
	}

	// Extract topic name from path
	parts := strings.Split(r.URL.Path, "/")
	if len(parts) < 3 {
		http.Error(w, "Invalid topic path", http.StatusBadRequest)
		return
	}
	topicName := parts[2]

	// Handle different operations based on the path
	switch {
	case strings.HasSuffix(r.URL.Path, "/priority"):
		s.handlePriorityMessage(w, r, topicName)
	case strings.HasSuffix(r.URL.Path, "/schedule"):
		s.handleScheduledMessage(w, r, topicName)
	case strings.HasSuffix(r.URL.Path, "/dlq"):
		s.handleDeadLetterQueue(w, r, topicName)
	default:
		http.Error(w, "Unknown operation", http.StatusNotFound)
	}
}

func (s *HTTPServer) handlePriorityMessage(w http.ResponseWriter, r *http.Request, topicName string) {
	if r.Method != http.MethodPost {
		http.Error(w, "Method not allowed", http.StatusMethodNotAllowed)
		return
	}

	var req struct {
		Data     []byte `json:"data"`
		Priority int    `json:"priority"`
	}
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
		http.Error(w, "Invalid request body", http.StatusBadRequest)
		return
	}

	// Use partition 0 for priority messages
	if err := s.manager.PublishMessage(topicName, 0, req.Data, req.Priority); err != nil {
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}

	w.WriteHeader(http.StatusCreated)
}

func (s *HTTPServer) handleScheduledMessage(w http.ResponseWriter, r *http.Request, topicName string) {
	if r.Method != http.MethodPost {
		http.Error(w, "Method not allowed", http.StatusMethodNotAllowed)
		return
	}

	var req struct {
		Data      []byte `json:"data"`
		Delay     int    `json:"delay"` // Delay in milliseconds
		Partition int    `json:"partition"`
	}
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
		http.Error(w, "Invalid request body", http.StatusBadRequest)
		return
	}

	// Schedule the message using the scheduler
	if err := s.scheduler.ScheduleMessage(topicName, req.Partition, req.Data, time.Duration(req.Delay)*time.Millisecond); err != nil {
		http.Error(w, fmt.Sprintf("Failed to schedule message: %v", err), http.StatusInternalServerError)
		return
	}

	w.WriteHeader(http.StatusCreated)
}

func (s *HTTPServer) handleDeadLetterQueue(w http.ResponseWriter, r *http.Request, topicName string) {
	if r.Method != http.MethodPost {
		http.Error(w, "Method not allowed", http.StatusMethodNotAllowed)
		return
	}

	var req struct {
		Data      []byte `json:"data"`
		Partition int    `json:"partition"`
		Reason    string `json:"reason"`
	}
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
		http.Error(w, "Invalid request body", http.StatusBadRequest)
		return
	}

	// Create DLQ topic name
	dlqTopic := fmt.Sprintf("%s-dlq", topicName)

	// Ensure DLQ topic exists
	if err := s.manager.CreateTopic(dlqTopic, 1); err != nil {
		http.Error(w, fmt.Sprintf("Failed to create DLQ topic: %v", err), http.StatusInternalServerError)
		return
	}

	// Publish message to DLQ with reason
	dlqMessage := map[string]interface{}{
		"original_topic": topicName,
		"partition":      req.Partition,
		"data":           req.Data,
		"reason":         req.Reason,
		"timestamp":      time.Now(),
	}

	dlqData, err := json.Marshal(dlqMessage)
	if err != nil {
		http.Error(w, fmt.Sprintf("Failed to marshal DLQ message: %v", err), http.StatusInternalServerError)
		return
	}

	if err := s.manager.PublishMessage(dlqTopic, 0, dlqData, 1); err != nil {
		http.Error(w, fmt.Sprintf("Failed to publish to DLQ: %v", err), http.StatusInternalServerError)
		return
	}

	w.WriteHeader(http.StatusCreated)
}

func (s *HTTPServer) handleDLQOperations(w http.ResponseWriter, r *http.Request) {
	if s.metrics != nil {
		s.metrics.ActiveConnections.Inc()
		defer s.metrics.ActiveConnections.Dec()
	}

	// Extract topic name from path
	parts := strings.Split(r.URL.Path, "/")
	if len(parts) < 3 {
		http.Error(w, "Invalid DLQ path", http.StatusBadRequest)
		return
	}
	topicName := parts[2]

	switch r.Method {
	case http.MethodGet:
		// Get messages from DLQ
		dlqTopic := fmt.Sprintf("%s-dlq", topicName)
		messages, err := s.manager.GetDLQMessages(dlqTopic)
		if err != nil {
			http.Error(w, err.Error(), http.StatusInternalServerError)
			return
		}
		json.NewEncoder(w).Encode(messages)

	case http.MethodPost:
		// Move message to DLQ
		var req struct {
			Data      []byte `json:"data"`
			Partition int    `json:"partition"`
			Reason    string `json:"reason"`
		}
		if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
			http.Error(w, "Invalid request body", http.StatusBadRequest)
			return
		}

		dlqTopic := fmt.Sprintf("%s-dlq", topicName)
		// Ensure DLQ topic exists with 1 partition
		if err := s.manager.CreateTopic(dlqTopic, 1); err != nil && !strings.Contains(err.Error(), "already exists") {
			http.Error(w, fmt.Sprintf("Failed to create DLQ topic: %v", err), http.StatusInternalServerError)
			return
		}

		// Create DLQ message with metadata
		dlqMessage := map[string]interface{}{
			"original_topic": topicName,
			"partition":      req.Partition,
			"data":           req.Data,
			"reason":         req.Reason,
			"timestamp":      time.Now(),
		}

		dlqData, err := json.Marshal(dlqMessage)
		if err != nil {
			http.Error(w, fmt.Sprintf("Failed to marshal DLQ message: %v", err), http.StatusInternalServerError)
			return
		}

		if err := s.manager.PublishMessage(dlqTopic, 0, dlqData, 1); err != nil {
			http.Error(w, fmt.Sprintf("Failed to publish to DLQ: %v", err), http.StatusInternalServerError)
			return
		}

		w.WriteHeader(http.StatusCreated)

	default:
		http.Error(w, "Method not allowed", http.StatusMethodNotAllowed)
	}
}

func (s *HTTPServer) handleConsumerGroups(w http.ResponseWriter, r *http.Request) {
	if s.metrics != nil {
		s.metrics.ActiveConnections.Inc()
		defer s.metrics.ActiveConnections.Dec()
	}

	switch r.Method {
	case http.MethodPost:
		var req struct {
			Name   string   `json:"name"`
			Topics []string `json:"topics"`
		}
		if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
			http.Error(w, "Invalid request body", http.StatusBadRequest)
			return
		}

		if err := s.manager.CreateConsumerGroup(req.Name, req.Topics); err != nil {
			http.Error(w, err.Error(), http.StatusInternalServerError)
			return
		}

		w.WriteHeader(http.StatusCreated)

	case http.MethodGet:
		groups := s.manager.ListConsumerGroups()
		json.NewEncoder(w).Encode(groups)

	default:
		http.Error(w, "Method not allowed", http.StatusMethodNotAllowed)
	}
}

func (s *HTTPServer) handleGroupOperations(w http.ResponseWriter, r *http.Request) {
	if s.metrics != nil {
		s.metrics.ActiveConnections.Inc()
		defer s.metrics.ActiveConnections.Dec()
	}

	parts := strings.Split(r.URL.Path, "/")
	if len(parts) < 3 {
		http.Error(w, "Invalid group path", http.StatusBadRequest)
		return
	}
	groupName := parts[2]

	if len(parts) == 4 && parts[3] == "join" {
		s.handleGroupJoin(w, r, groupName)
		return
	}

	http.Error(w, "Unknown operation", http.StatusNotFound)
}

func (s *HTTPServer) handleGroupJoin(w http.ResponseWriter, r *http.Request, groupName string) {
	if r.Method != http.MethodPost {
		http.Error(w, "Method not allowed", http.StatusMethodNotAllowed)
		return
	}

	memberID := uuid.New().String()
	if err := s.manager.JoinConsumerGroup(groupName, memberID); err != nil {
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}

	response := map[string]string{
		"member_id": memberID,
	}
	w.WriteHeader(http.StatusOK)
	json.NewEncoder(w).Encode(response)
}
