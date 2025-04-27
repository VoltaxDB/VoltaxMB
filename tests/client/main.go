package main

import (
	"bytes"
	"encoding/json"
	"flag"
	"fmt"
	"io"
	"log"
	"net/http"
	"time"
)

var (
	brokerHost  = flag.String("broker", "http://localhost:8080", "Broker HTTP host")
	metricsHost = flag.String("metrics", "http://localhost:9091", "Metrics server host")
	cleanup     = flag.Bool("cleanup", true, "Clean up test resources after tests")
	verbose     = flag.Bool("verbose", false, "Enable verbose logging")
)

type Message struct {
	ID        string    `json:"id"`
	Topic     string    `json:"topic"`
	Data      []byte    `json:"data"`
	Timestamp time.Time `json:"timestamp"`
}

type TopicResponse struct {
	Name       string `json:"name"`
	Partitions int    `json:"partitions"`
}

type ErrorResponse struct {
	Error string `json:"error"`
}

func main() {
	flag.Parse()
	log.Println("Starting VoltaxMB Test Client...")
	log.Printf("Broker host: %s", *brokerHost)
	log.Printf("Metrics host: %s", *metricsHost)

	var createdTopics []string
	defer func() {
		if *cleanup {
			cleanupResources(createdTopics)
		}
	}()

	topic := "test-topic"
	if err := testCreateTopic(topic, 3); err != nil {
		log.Fatalf("Topic creation test failed: %v", err)
	}
	createdTopics = append(createdTopics, topic)
	log.Println("✅ Topic creation test passed")

	message := []byte("Hello, VoltaxMB!")
	if err := testPublishMessage(topic, 0, message); err != nil {
		log.Fatalf("Message publishing test failed: %v", err)
	}
	log.Println("✅ Message publishing test passed")

	// Test 3: Message Consumption
	if err := testConsumeMessage(topic, 0); err != nil {
		log.Fatalf("Message consumption test failed: %v", err)
	}
	log.Println("✅ Message consumption test passed")

	// Test 4: Priority Queue
	priorityTopic := "priority-topic"
	if err := testPriorityQueue(priorityTopic); err != nil {
		log.Fatalf("Priority queue test failed: %v", err)
	}
	createdTopics = append(createdTopics, priorityTopic)
	log.Println("✅ Priority queue test passed")

	// Test 5: Scheduled Messages
	scheduledTopic := "scheduled-topic"
	if err := testScheduledMessages(scheduledTopic); err != nil {
		log.Fatalf("Scheduled messages test failed: %v", err)
	}
	createdTopics = append(createdTopics, scheduledTopic)
	log.Println("✅ Scheduled messages test passed")

	// Test 6: Dead Letter Queue
	dlqTopic := "dlq-topic"
	if err := testDeadLetterQueue(dlqTopic); err != nil {
		log.Fatalf("Dead letter queue test failed: %v", err)
	}
	createdTopics = append(createdTopics, dlqTopic)
	log.Println("✅ Dead letter queue test passed")

	// Test 7: Consumer Groups
	groupTopic := "group-topic"
	if err := testConsumerGroups(groupTopic); err != nil {
		log.Fatalf("Consumer groups test failed: %v", err)
	}
	createdTopics = append(createdTopics, groupTopic)
	log.Println("✅ Consumer groups test passed")

	// Test 8: Metrics
	if err := testMetrics(); err != nil {
		log.Fatalf("Metrics test failed: %v", err)
	}
	log.Println("✅ Metrics test passed")

	log.Println("🎉 All tests completed successfully!")
}

func logVerbose(format string, args ...interface{}) {
	if *verbose {
		log.Printf(format, args...)
	}
}

func testCreateTopic(topic string, partitions int) error {
	url := fmt.Sprintf("%s/api/topics", *brokerHost)
	payload := map[string]interface{}{
		"name":           topic,
		"num_partitions": partitions,
	}

	jsonData, err := json.Marshal(payload)
	if err != nil {
		return fmt.Errorf("failed to marshal topic creation payload: %v", err)
	}

	logVerbose("Creating topic: %s with %d partitions", topic, partitions)
	logVerbose("Request URL: %s", url)
	logVerbose("Request payload: %s", string(jsonData))

	resp, err := http.Post(url, "application/json", bytes.NewBuffer(jsonData))
	if err != nil {
		return fmt.Errorf("failed to create topic: %v", err)
	}
	defer resp.Body.Close()

	// Read the response body
	body, err := io.ReadAll(resp.Body)
	if err != nil {
		return fmt.Errorf("failed to read response body: %v", err)
	}

	logVerbose("Response status: %d", resp.StatusCode)
	logVerbose("Response body: %s", string(body))

	if resp.StatusCode != http.StatusCreated {
		if len(body) > 0 {
			var errResp ErrorResponse
			if err := json.Unmarshal(body, &errResp); err == nil {
				return fmt.Errorf("failed to create topic: %s", errResp.Error)
			}
		}
		return fmt.Errorf("unexpected status code: %d", resp.StatusCode)
	}

	// Only try to decode if we have a response body
	if len(body) > 0 {
		var topicResp TopicResponse
		if err := json.Unmarshal(body, &topicResp); err != nil {
			return fmt.Errorf("failed to decode topic response: %v (body: %s)", err, string(body))
		}

		if topicResp.Name != topic || topicResp.Partitions != partitions {
			return fmt.Errorf("topic creation response mismatch: got %+v, want name=%s partitions=%d",
				topicResp, topic, partitions)
		}
	}

	return nil
}

func testPublishMessage(topic string, partition int, data []byte) error {
	url := fmt.Sprintf("%s/api/publish", *brokerHost)
	payload := map[string]interface{}{
		"topic":     topic,
		"partition": partition,
		"data":      data,
		"priority":  1,
	}

	jsonData, err := json.Marshal(payload)
	if err != nil {
		return fmt.Errorf("failed to marshal publish payload: %v", err)
	}

	logVerbose("Publishing message to topic: %s, partition: %d", topic, partition)
	logVerbose("Request URL: %s", url)
	logVerbose("Request payload: %s", string(jsonData))

	resp, err := http.Post(url, "application/json", bytes.NewBuffer(jsonData))
	if err != nil {
		return fmt.Errorf("failed to publish message: %v", err)
	}
	defer resp.Body.Close()

	// Read the response body
	body, err := io.ReadAll(resp.Body)
	if err != nil {
		return fmt.Errorf("failed to read response body: %v", err)
	}

	logVerbose("Response status: %d", resp.StatusCode)
	logVerbose("Response body: %s", string(body))

	if resp.StatusCode != http.StatusCreated {
		if len(body) > 0 {
			var errResp ErrorResponse
			if err := json.Unmarshal(body, &errResp); err == nil {
				return fmt.Errorf("failed to publish message: %s", errResp.Error)
			}
		}
		return fmt.Errorf("unexpected status code: %d", resp.StatusCode)
	}

	return nil
}

func testConsumeMessage(topic string, partition int) error {
	url := fmt.Sprintf("%s/api/consume?topic=%s&partition=%d", *brokerHost, topic, partition)

	resp, err := http.Get(url)
	if err != nil {
		return fmt.Errorf("failed to consume message: %v", err)
	}
	defer resp.Body.Close()

	if resp.StatusCode == http.StatusNoContent {
		return nil
	}

	if resp.StatusCode != http.StatusOK {
		var errResp ErrorResponse
		if err := json.NewDecoder(resp.Body).Decode(&errResp); err != nil {
			return fmt.Errorf("unexpected status code: %d", resp.StatusCode)
		}
		return fmt.Errorf("failed to consume message: %s", errResp.Error)
	}

	var message Message
	if err := json.NewDecoder(resp.Body).Decode(&message); err != nil {
		return fmt.Errorf("failed to decode message: %v", err)
	}

	if message.Topic != topic {
		return fmt.Errorf("message topic mismatch: got %s, want %s", message.Topic, topic)
	}

	return nil
}

func testPriorityQueue(topic string) error {
	if err := testCreateTopic(topic, 1); err != nil {
		return fmt.Errorf("failed to create priority topic: %v", err)
	}

	// Create messages with different priorities
	messages := []struct {
		data     []byte
		priority int
	}{
		{[]byte("Low priority"), 1},
		{[]byte("High priority"), 3},
		{[]byte("Medium priority"), 2},
	}

	for _, msg := range messages {
		url := fmt.Sprintf("%s/topics/%s/priority", *brokerHost, topic)
		payload := map[string]interface{}{
			"data":     msg.data,
			"priority": msg.priority,
		}

		jsonData, err := json.Marshal(payload)
		if err != nil {
			return fmt.Errorf("failed to marshal priority message payload: %v", err)
		}

		resp, err := http.Post(url, "application/json", bytes.NewBuffer(jsonData))
		if err != nil {
			return fmt.Errorf("failed to publish priority message: %v", err)
		}
		resp.Body.Close()

		if resp.StatusCode != http.StatusCreated {
			var errResp ErrorResponse
			if err := json.NewDecoder(resp.Body).Decode(&errResp); err != nil {
				return fmt.Errorf("unexpected status code: %d", resp.StatusCode)
			}
			return fmt.Errorf("failed to publish priority message: %s", errResp.Error)
		}
	}

	return nil
}

func testScheduledMessages(topic string) error {
	if err := testCreateTopic(topic, 1); err != nil {
		return fmt.Errorf("failed to create scheduled topic: %v", err)
	}

	// Schedule a message for 5 seconds in the future
	url := fmt.Sprintf("%s/topics/%s/schedule", *brokerHost, topic)
	payload := map[string]interface{}{
		"data":  []byte("Scheduled message"),
		"delay": 5000, // 5 seconds in milliseconds
	}

	jsonData, err := json.Marshal(payload)
	if err != nil {
		return fmt.Errorf("failed to marshal scheduled message payload: %v", err)
	}

	resp, err := http.Post(url, "application/json", bytes.NewBuffer(jsonData))
	if err != nil {
		return fmt.Errorf("failed to schedule message: %v", err)
	}
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusCreated {
		var errResp ErrorResponse
		if err := json.NewDecoder(resp.Body).Decode(&errResp); err != nil {
			return fmt.Errorf("unexpected status code: %d", resp.StatusCode)
		}
		return fmt.Errorf("failed to schedule message: %s", errResp.Error)
	}

	// Wait for the message to be delivered
	time.Sleep(6 * time.Second)

	return nil
}

func testDeadLetterQueue(topic string) error {
	if err := testCreateTopic(topic, 1); err != nil {
		return fmt.Errorf("failed to create DLQ topic: %v", err)
	}

	// Publish a message that will fail processing
	url := fmt.Sprintf("%s/topics/%s/dlq", *brokerHost, topic)
	payload := map[string]interface{}{
		"data": []byte("Invalid message"),
	}

	jsonData, err := json.Marshal(payload)
	if err != nil {
		return fmt.Errorf("failed to marshal DLQ message payload: %v", err)
	}

	resp, err := http.Post(url, "application/json", bytes.NewBuffer(jsonData))
	if err != nil {
		return fmt.Errorf("failed to publish DLQ message: %v", err)
	}
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusCreated {
		var errResp ErrorResponse
		if err := json.NewDecoder(resp.Body).Decode(&errResp); err != nil {
			return fmt.Errorf("unexpected status code: %d", resp.StatusCode)
		}
		return fmt.Errorf("failed to publish DLQ message: %s", errResp.Error)
	}

	// Check DLQ
	dlqURL := fmt.Sprintf("%s/dlq/%s", *brokerHost, topic)
	resp, err = http.Get(dlqURL)
	if err != nil {
		return fmt.Errorf("failed to check DLQ: %v", err)
	}
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusOK {
		var errResp ErrorResponse
		if err := json.NewDecoder(resp.Body).Decode(&errResp); err != nil {
			return fmt.Errorf("unexpected status code: %d", resp.StatusCode)
		}
		return fmt.Errorf("failed to check DLQ: %s", errResp.Error)
	}

	return nil
}

func testConsumerGroups(topic string) error {
	if err := testCreateTopic(topic, 1); err != nil {
		return fmt.Errorf("failed to create consumer group topic: %v", err)
	}

	// Create a consumer group
	url := fmt.Sprintf("%s/groups", *brokerHost)
	payload := map[string]interface{}{
		"name":   "test-group",
		"topics": []string{topic},
	}

	jsonData, err := json.Marshal(payload)
	if err != nil {
		return fmt.Errorf("failed to marshal consumer group payload: %v", err)
	}

	resp, err := http.Post(url, "application/json", bytes.NewBuffer(jsonData))
	if err != nil {
		return fmt.Errorf("failed to create consumer group: %v", err)
	}
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusCreated {
		var errResp ErrorResponse
		if err := json.NewDecoder(resp.Body).Decode(&errResp); err != nil {
			return fmt.Errorf("unexpected status code: %d", resp.StatusCode)
		}
		return fmt.Errorf("failed to create consumer group: %s", errResp.Error)
	}

	// Join the group
	joinURL := fmt.Sprintf("%s/groups/test-group/join", *brokerHost)
	resp, err = http.Post(joinURL, "application/json", nil)
	if err != nil {
		return fmt.Errorf("failed to join consumer group: %v", err)
	}
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusOK {
		var errResp ErrorResponse
		if err := json.NewDecoder(resp.Body).Decode(&errResp); err != nil {
			return fmt.Errorf("unexpected status code: %d", resp.StatusCode)
		}
		return fmt.Errorf("failed to join consumer group: %s", errResp.Error)
	}

	return nil
}

func testMetrics() error {
	resp, err := http.Get(*metricsHost + "/metrics")
	if err != nil {
		return fmt.Errorf("failed to get metrics: %v", err)
	}
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusOK {
		var errResp ErrorResponse
		if err := json.NewDecoder(resp.Body).Decode(&errResp); err != nil {
			return fmt.Errorf("unexpected status code: %d", resp.StatusCode)
		}
		return fmt.Errorf("failed to get metrics: %s", errResp.Error)
	}

	return nil
}

func cleanupResources(topics []string) {
	log.Println("Cleaning up test resources...")
	for _, topic := range topics {
		url := fmt.Sprintf("%s/api/topics?name=%s", *brokerHost, topic)
		req, err := http.NewRequest(http.MethodDelete, url, nil)
		if err != nil {
			log.Printf("Failed to create delete request for topic %s: %v", topic, err)
			continue
		}

		resp, err := http.DefaultClient.Do(req)
		if err != nil {
			log.Printf("Failed to delete topic %s: %v", topic, err)
			continue
		}
		resp.Body.Close()

		if resp.StatusCode != http.StatusOK {
			log.Printf("Failed to delete topic %s: unexpected status code %d", topic, resp.StatusCode)
		}
	}
	log.Println("Cleanup completed")
}
