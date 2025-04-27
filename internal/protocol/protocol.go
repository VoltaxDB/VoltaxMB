package protocol

import (
	"encoding/binary"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net"
	"time"
)

// Command types
const (
	CmdConnect     = 1
	CmdPublish     = 2
	CmdConsume     = 3
	CmdAck         = 4
	CmdCreateTopic = 5
	CmdSchedule    = 6
	CmdDLQ         = 7
)

// Error types
var (
	ErrUnknownFrameType = errors.New("unknown frame type")
	ErrInvalidFrame     = errors.New("invalid frame")
	ErrConnectionClosed = errors.New("connection closed")
)

// Frame represents a protocol frame
type Frame struct {
	Command uint8
	Length  uint32
	Payload []byte
}

// ConnectRequest represents a connection request
type ConnectRequest struct {
	ClientID string `json:"client_id"`
	Username string `json:"username"`
	Password string `json:"password"`
}

// PublishRequest represents a publish request
type PublishRequest struct {
	Topic     string `json:"topic"`
	Partition int    `json:"partition"`
	Data      string `json:"data"`
	Priority  int    `json:"priority"`
}

// ConsumeRequest represents a consume request
type ConsumeRequest struct {
	Topic     string `json:"topic"`
	Partition int    `json:"partition"`
}

// AckRequest represents an acknowledgment request
type AckRequest struct {
	MessageID string `json:"message_id"`
}

// CreateTopicRequest represents a topic creation request
type CreateTopicRequest struct {
	Name          string `json:"name"`
	NumPartitions int    `json:"num_partitions"`
}

// ScheduleRequest represents a scheduled message request
type ScheduleRequest struct {
	Topic     string `json:"topic"`
	Partition int    `json:"partition"`
	Data      string `json:"data"`
	Delay     int    `json:"delay"` // Delay in milliseconds
}

// DLQRequest represents a Dead Letter Queue request
type DLQRequest struct {
	Topic     string `json:"topic"`
	Partition int    `json:"partition"`
	Data      string `json:"data"`
	Reason    string `json:"reason"`
}

// Response represents a protocol response
type Response struct {
	Success bool        `json:"success"`
	Error   string      `json:"error,omitempty"`
	Data    interface{} `json:"data,omitempty"`
}

// ReadFrame reads a frame from the connection
func ReadFrame(conn io.Reader) (*Frame, error) {
	// Read command
	cmdBuf := make([]byte, 1)
	if _, err := io.ReadFull(conn, cmdBuf); err != nil {
		if err == io.EOF {
			return nil, ErrConnectionClosed
		}
		return nil, fmt.Errorf("failed to read command: %v", err)
	}

	// Read length
	lenBuf := make([]byte, 4)
	if _, err := io.ReadFull(conn, lenBuf); err != nil {
		if err == io.EOF {
			return nil, ErrConnectionClosed
		}
		return nil, fmt.Errorf("failed to read length: %v", err)
	}
	length := binary.BigEndian.Uint32(lenBuf)

	// Read payload
	payload := make([]byte, length)
	if length > 0 {
		if _, err := io.ReadFull(conn, payload); err != nil {
			if err == io.EOF {
				return nil, ErrConnectionClosed
			}
			return nil, fmt.Errorf("failed to read payload: %v", err)
		}
	}

	return &Frame{
		Command: cmdBuf[0],
		Length:  length,
		Payload: payload,
	}, nil
}

// WriteResponse writes a response to the connection
func WriteResponse(w io.Writer, success bool, errMsg string, data interface{}) error {
	response := Response{
		Success: success,
		Error:   errMsg,
		Data:    data,
	}

	// Marshal response
	payload, err := json.Marshal(response)
	if err != nil {
		return fmt.Errorf("failed to marshal response: %v", err)
	}

	// Write length
	length := uint32(len(payload))
	lenBuf := make([]byte, 4)
	binary.BigEndian.PutUint32(lenBuf, length)
	if _, err := w.Write(lenBuf); err != nil {
		return fmt.Errorf("failed to write length: %v", err)
	}

	// Write payload
	if length > 0 {
		if _, err := w.Write(payload); err != nil {
			return fmt.Errorf("failed to write payload: %v", err)
		}
	}

	return nil
}

// SetReadDeadline sets the read deadline for the connection
func SetReadDeadline(conn net.Conn, timeout time.Duration) {
	conn.SetReadDeadline(time.Now().Add(timeout))
}
