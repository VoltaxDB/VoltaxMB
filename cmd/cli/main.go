package main

import (
	"bytes"
	"encoding/json"
	"flag"
	"fmt"
	"net"
	"net/http"
	"os"
	"time"
)

var (
	brokerURL string
	useTCP    bool
	tcpAddr   string
)

func init() {
	flag.StringVar(&brokerURL, "broker", "http://localhost:8080", "broker URL")
	flag.BoolVar(&useTCP, "tcp", false, "use TCP connection")
	flag.StringVar(&tcpAddr, "tcp-addr", "localhost:8081", "TCP server address")
	flag.Parse()
}

func main() {
	if len(os.Args) < 2 {
		printUsage()
		os.Exit(1)
	}

	command := os.Args[1]
	switch command {
	case "create-topic":
		if len(os.Args) < 4 {
			fmt.Println("Usage: vtxmq-cli create-topic <topic-name> <num-partitions>")
			os.Exit(1)
		}
		createTopic(os.Args[2], os.Args[3])
	case "publish":
		if len(os.Args) < 5 {
			fmt.Println("Usage: vtxmq-cli publish <topic> <partition> <message>")
			os.Exit(1)
		}
		publishMessage(os.Args[2], os.Args[3], os.Args[4])
	case "consume":
		if len(os.Args) < 4 {
			fmt.Println("Usage: vtxmq-cli consume <topic> <partition>")
			os.Exit(1)
		}
		consumeMessage(os.Args[2], os.Args[3])
	default:
		printUsage()
		os.Exit(1)
	}
}

func printUsage() {
	fmt.Println("Usage: vtxmq-cli <command> [args]")
	fmt.Println("\nCommands:")
	fmt.Println("  create-topic <topic-name> <num-partitions>")
	fmt.Println("  publish <topic> <partition> <message>")
	fmt.Println("  consume <topic> <partition>")
}

func createTopic(name, numPartitions string) {
	req := struct {
		Name          string `json:"name"`
		NumPartitions int    `json:"num_partitions"`
	}{
		Name:          name,
		NumPartitions: parseInt(numPartitions),
	}

	resp, err := sendRequest("POST", "/api/topics", req)
	if err != nil {
		fmt.Printf("Error creating topic: %v\n", err)
		os.Exit(1)
	}
	defer resp.Body.Close()

	if resp.StatusCode == http.StatusCreated {
		fmt.Println("Topic created successfully")
	} else {
		fmt.Printf("Failed to create topic: %s\n", resp.Status)
	}
}

func publishMessage(topic, partition, message string) {
	req := struct {
		Topic     string `json:"topic"`
		Partition int    `json:"partition"`
		Data      []byte `json:"data"`
		Priority  int    `json:"priority"`
	}{
		Topic:     topic,
		Partition: parseInt(partition),
		Data:      []byte(message),
		Priority:  0,
	}

	resp, err := sendRequest("POST", "/api/publish", req)
	if err != nil {
		fmt.Printf("Error publishing message: %v\n", err)
		os.Exit(1)
	}
	defer resp.Body.Close()

	if resp.StatusCode == http.StatusCreated {
		fmt.Println("Message published successfully")
	} else {
		fmt.Printf("Failed to publish message: %s\n", resp.Status)
	}
}

func consumeMessage(topic, partition string) {
	url := fmt.Sprintf("%s/api/consume?topic=%s&partition=%s", brokerURL, topic, partition)
	resp, err := http.Get(url)
	if err != nil {
		fmt.Printf("Error consuming message: %v\n", err)
		os.Exit(1)
	}
	defer resp.Body.Close()

	if resp.StatusCode == http.StatusNoContent {
		fmt.Println("No messages available")
		return
	}

	if resp.StatusCode != http.StatusOK {
		fmt.Printf("Failed to consume message: %s\n", resp.Status)
		return
	}

	var message struct {
		ID        string `json:"id"`
		Topic     string `json:"topic"`
		Partition int    `json:"partition"`
		Data      []byte `json:"data"`
		Timestamp string `json:"timestamp"`
	}

	if err := json.NewDecoder(resp.Body).Decode(&message); err != nil {
		fmt.Printf("Error decoding message: %v\n", err)
		return
	}

	fmt.Printf("Message ID: %s\n", message.ID)
	fmt.Printf("Topic: %s\n", message.Topic)
	fmt.Printf("Partition: %d\n", message.Partition)
	fmt.Printf("Data: %s\n", string(message.Data))
	fmt.Printf("Timestamp: %s\n", message.Timestamp)
}

func sendRequest(method, path string, body interface{}) (*http.Response, error) {
	if useTCP {
		return sendTCPRequest(method, path, body)
	}
	return sendHTTPRequest(method, path, body)
}

func sendTCPRequest(method, path string, body interface{}) (*http.Response, error) {
	conn, err := net.Dial("tcp", tcpAddr)
	if err != nil {
		return nil, fmt.Errorf("failed to connect to TCP server: %v", err)
	}
	defer conn.Close()

	conn.SetDeadline(time.Now().Add(5 * time.Second))

	// Create request
	req := struct {
		Method string      `json:"method"`
		Path   string      `json:"path"`
		Body   interface{} `json:"body"`
	}{
		Method: method,
		Path:   path,
		Body:   body,
	}

	encoder := json.NewEncoder(conn)
	if err := encoder.Encode(req); err != nil {
		return nil, fmt.Errorf("failed to encode request: %v", err)
	}

	var response http.Response
	decoder := json.NewDecoder(conn)
	if err := decoder.Decode(&response); err != nil {
		return nil, fmt.Errorf("failed to decode response: %v", err)
	}

	return &response, nil
}

func sendHTTPRequest(method, path string, body interface{}) (*http.Response, error) {
	data, err := json.Marshal(body)
	if err != nil {
		return nil, err
	}

	req, err := http.NewRequest(method, brokerURL+path, bytes.NewBuffer(data))
	if err != nil {
		return nil, err
	}

	req.Header.Set("Content-Type", "application/json")
	return http.DefaultClient.Do(req)
}

func parseInt(s string) int {
	var i int
	fmt.Sscanf(s, "%d", &i)
	return i
}
