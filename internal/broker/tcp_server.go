package broker

import (
	"encoding/json"
	"log"
	"net"
	"sync"
	"time"

	"github.com/voltaxmq/voltaxmq/internal/metrics"
	"github.com/voltaxmq/voltaxmq/internal/persistence"
	"github.com/voltaxmq/voltaxmq/internal/protocol"
	"github.com/voltaxmq/voltaxmq/internal/queue"
)

type TCPServer struct {
	queueManager *queue.Manager
	aofWriter    *persistence.AOFWriter
	metrics      *metrics.Metrics
	clients      map[string]*Client
	mu           sync.RWMutex
	stopCh       chan struct{}
}

func NewTCPServer(queueManager *queue.Manager, aofWriter *persistence.AOFWriter) *TCPServer {
	return &TCPServer{
		queueManager: queueManager,
		aofWriter:    aofWriter,
		clients:      make(map[string]*Client),
		stopCh:       make(chan struct{}),
	}
}

func (s *TCPServer) SetMetrics(metrics *metrics.Metrics) {
	s.metrics = metrics
}

func (s *TCPServer) HandleConnection(conn net.Conn) {
	client := &Client{
		Conn: conn,
	}
	defer s.removeClient(client)

	if s.metrics != nil {
		s.metrics.ActiveConnections.Inc()
		defer s.metrics.ActiveConnections.Dec()
	}

	// Set read deadline
	conn.SetReadDeadline(time.Now().Add(30 * time.Second))

	for {
		select {
		case <-s.stopCh:
			return
		default:
			frame, err := protocol.ReadFrame(conn)
			if err != nil {
				log.Printf("Error reading frame: %v", err)
				return
			}

			// Reset read deadline after successful read
			conn.SetReadDeadline(time.Now().Add(30 * time.Second))

			if err := s.handleFrame(client, frame); err != nil {
				log.Printf("Error handling frame: %v", err)
				protocol.WriteResponse(conn, false, err.Error(), nil)
				return
			}
		}
	}
}

type Client struct {
	Conn net.Conn
}

func (s *TCPServer) removeClient(client *Client) {
	s.mu.Lock()
	defer s.mu.Unlock()

	for id, c := range s.clients {
		if c == client {
			delete(s.clients, id)
			break
		}
	}
	client.Conn.Close()
}

func (s *TCPServer) handleFrame(client *Client, frame *protocol.Frame) error {
	switch frame.Command {
	case protocol.CmdPublish:
		var req protocol.PublishRequest
		if err := json.Unmarshal(frame.Payload, &req); err != nil {
			return err
		}

		// Convert string data to []byte
		data := []byte(req.Data)
		if err := s.queueManager.PublishMessage(req.Topic, req.Partition, data, req.Priority); err != nil {
			return err
		}

		if s.aofWriter != nil {
			msg := &queue.Message{
				Topic:     req.Topic,
				Partition: req.Partition,
				Data:      data,
				Priority:  req.Priority,
			}
			if err := s.aofWriter.WriteMessage(msg); err != nil {
				log.Printf("Warning: Failed to write to AOF: %v", err)
			}
		}

		return protocol.WriteResponse(client.Conn, true, "", nil)

	case protocol.CmdConsume:
		var req protocol.ConsumeRequest
		if err := json.Unmarshal(frame.Payload, &req); err != nil {
			return err
		}

		msg, err := s.queueManager.ConsumeMessage(req.Topic, req.Partition)
		if err != nil {
			return err
		}

		// Convert []byte data to string for JSON response
		if msg != nil {
			msg.Data = []byte(string(msg.Data))
		}

		return protocol.WriteResponse(client.Conn, true, "", msg)

	case protocol.CmdCreateTopic:
		var req protocol.CreateTopicRequest
		if err := json.Unmarshal(frame.Payload, &req); err != nil {
			return err
		}

		if err := s.queueManager.CreateTopic(req.Name, req.NumPartitions); err != nil {
			return err
		}

		if s.aofWriter != nil {
			if err := s.aofWriter.WriteTopicCreation(req.Name, req.NumPartitions); err != nil {
				log.Printf("Warning: Failed to write to AOF: %v", err)
			}
		}

		return protocol.WriteResponse(client.Conn, true, "", nil)

	case protocol.CmdSchedule:
		var req protocol.ScheduleRequest
		if err := json.Unmarshal(frame.Payload, &req); err != nil {
			return err
		}

		// Convert string data to []byte
		data := []byte(req.Data)
		if err := s.queueManager.ScheduleMessage(req.Topic, req.Partition, data, req.Delay); err != nil {
			return err
		}

		if s.aofWriter != nil {
			msg := &queue.Message{
				Topic:     req.Topic,
				Partition: req.Partition,
				Data:      data,
				Priority:  1, // Default priority for scheduled messages
			}
			if err := s.aofWriter.WriteMessage(msg); err != nil {
				log.Printf("Warning: Failed to write to AOF: %v", err)
			}
		}

		return protocol.WriteResponse(client.Conn, true, "", nil)

	case protocol.CmdDLQ:
		var req protocol.DLQRequest
		if err := json.Unmarshal(frame.Payload, &req); err != nil {
			return err
		}

		// Convert string data to []byte
		data := []byte(req.Data)
		if err := s.queueManager.MoveToDLQ(req.Topic, req.Partition, data, req.Reason); err != nil {
			return err
		}

		if s.aofWriter != nil {
			msg := &queue.Message{
				Topic:     req.Topic,
				Partition: req.Partition,
				Data:      data,
				Priority:  1, // Default priority for DLQ messages
			}
			if err := s.aofWriter.WriteMessage(msg); err != nil {
				log.Printf("Warning: Failed to write to AOF: %v", err)
			}
		}

		return protocol.WriteResponse(client.Conn, true, "", nil)

	default:
		return protocol.ErrUnknownFrameType
	}
}

func (s *TCPServer) Stop() {
	close(s.stopCh)
}
