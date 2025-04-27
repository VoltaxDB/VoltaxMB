package tests

import (
	"bytes"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"os"
	"os/exec"
	"testing"
)

func TestCLICommands(t *testing.T) {
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		switch r.URL.Path {
		case "/api/topics":
			if r.Method == "POST" {
				w.WriteHeader(http.StatusCreated)
				return
			}
		case "/api/publish":
			if r.Method == "POST" {
				w.WriteHeader(http.StatusCreated)
				return
			}
		case "/api/consume":
			if r.Method == "GET" {
				message := map[string]interface{}{
					"id":        "test-id",
					"topic":     "test-topic",
					"partition": 0,
					"data":      []byte("test message"),
					"timestamp": "2024-03-20T12:00:00Z",
				}
				json.NewEncoder(w).Encode(message)
				return
			}
		}
		w.WriteHeader(http.StatusNotFound)
	}))
	defer server.Close()

	os.Setenv("BROKER_URL", server.URL)

	tests := []struct {
		name     string
		args     []string
		expected string
	}{
		{
			name:     "Create Topic",
			args:     []string{"create-topic", "test-topic", "1"},
			expected: "Topic created successfully",
		},
		{
			name:     "Publish Message",
			args:     []string{"publish", "test-topic", "0", "test message"},
			expected: "Message published successfully",
		},
		{
			name:     "Consume Message",
			args:     []string{"consume", "test-topic", "0"},
			expected: "Message ID: test-id",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			cmd := exec.Command("./vtxmq-cli", tt.args...)
			var out bytes.Buffer
			cmd.Stdout = &out
			err := cmd.Run()
			if err != nil {
				t.Errorf("Command failed: %v", err)
			}
			if !bytes.Contains(out.Bytes(), []byte(tt.expected)) {
				t.Errorf("Expected output to contain %q, got %q", tt.expected, out.String())
			}
		})
	}
}
