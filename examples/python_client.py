import socket
import json
import struct
import time
import threading
from typing import Optional, Dict, Any, List

class VoltaxMBClient:
    def __init__(self, host: str = 'localhost', port: int = 9090):
        self.host = host
        self.port = port
        self.sock = None
        self.connected = False
        self.lock = threading.Lock()

    def connect(self) -> None:
        """Connect to the broker."""
        try:
            self.sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
            self.sock.connect((self.host, self.port))
            self.connected = True
            print(f"Connected to {self.host}:{self.port}")
        except Exception as e:
            print(f"Connection failed: {e}")
            raise

    def disconnect(self) -> None:
        """Disconnect from the broker."""
        if self.sock:
            self.sock.close()
            self.connected = False
            print("Disconnected from broker")

    def send_frame(self, cmd: int, payload: Dict[str, Any]) -> Dict[str, Any]:
        """Send a frame to the broker and receive response."""
        if not self.connected:
            raise ConnectionError("Not connected to broker")

        with self.lock:
            try:
                # Marshal payload
                data = json.dumps(payload).encode('utf-8')
                
                # Send command
                self.sock.send(bytes([cmd]))
                
                # Send length
                length = len(data)
                self.sock.send(struct.pack('>I', length))
                
                # Send payload
                self.sock.send(data)
                
                # Read response length
                length_bytes = self.sock.recv(4)
                if len(length_bytes) != 4:
                    raise ConnectionError("Failed to read response length")
                response_length = struct.unpack('>I', length_bytes)[0]
                
                # Read response payload
                response_data = b''
                while len(response_data) < response_length:
                    chunk = self.sock.recv(min(1024, response_length - len(response_data)))
                    if not chunk:
                        raise ConnectionError("Connection closed by server")
                    response_data += chunk
                
                # Parse response
                if response_length == 0:
                    return {}
                    
                response = json.loads(response_data.decode('utf-8'))
                if 'error' in response:
                    raise RuntimeError(response['error'])
                return response
            except Exception as e:
                print(f"Error sending frame: {e}")
                raise

    def create_topic(self, name: str, num_partitions: int) -> Dict[str, Any]:
        """Create a new topic."""
        payload = {
            "name": name,
            "num_partitions": num_partitions
        }
        return self.send_frame(5, payload)

    def publish(self, topic: str, partition: int, data: str, priority: int = 1) -> Dict[str, Any]:
        """Publish a message to a topic."""
        payload = {
            "topic": topic,
            "partition": partition,
            "data": data,
            "priority": priority
        }
        return self.send_frame(2, payload)

    def consume(self, topic: str, partition: int) -> Dict[str, Any]:
        """Consume a message from a topic."""
        payload = {
            "topic": topic,
            "partition": partition
        }
        return self.send_frame(3, payload)

    def acknowledge(self, message_id: str, topic: str, partition: int) -> Dict[str, Any]:
        """Acknowledge a message."""
        payload = {
            "message_id": message_id,
            "topic": topic,
            "partition": partition
        }
        return self.send_frame(4, payload)

    def schedule_message(self, topic: str, partition: int, data: str, delay_ms: int) -> Dict[str, Any]:
        """Schedule a message for future delivery."""
        payload = {
            "topic": topic,
            "partition": partition,
            "data": data,
            "delay": delay_ms
        }
        return self.send_frame(6, payload)

    def move_to_dlq(self, topic: str, partition: int, data: str, reason: str) -> Dict[str, Any]:
        """Move a message to Dead Letter Queue."""
        payload = {
            "topic": topic,
            "partition": partition,
            "data": data,
            "reason": reason
        }
        return self.send_frame(7, payload)

def test_tcp_endpoints():
    """Test all TCP endpoints."""
    client = VoltaxMBClient()
    
    try:
        # Connect to broker
        client.connect()
        
        # Test topic creation
        print("\nTesting topic creation...")
        response = client.create_topic("test-topic2", 3)
        print(f"Create topic response: {response}")
        
        # Test message publishing
        print("\nTesting message publishing...")
        response = client.publish("test-topic2", 0, "Hello, World!", 1)
        print(f"Publish response: {response}")
        
        # Test message consumption
        print("\nTesting message consumption...")
        response = client.consume("test-topic2", 0)
        print(f"Consume response: {response}")
        
        # Test message acknowledgment
        if response and 'id' in response:
            print("\nTesting message acknowledgment...")
            ack_response = client.acknowledge(response['id'], "test-topic2", 0)
            print(f"Acknowledge response: {ack_response}")
        
        # Test scheduled message
        print("\nTesting scheduled message...")
        response = client.schedule_message("test-topic2", 0, "Delayed message", 5000)
        print(f"Schedule message response: {response}")
        
        # Wait for scheduled message
        print("\nWaiting for scheduled message...")
        time.sleep(6)
        response = client.consume("test-topic2", 0)
        print(f"Consume scheduled message response: {response}")
        
        # Test DLQ
        print("\nTesting Dead Letter Queue...")
        response = client.move_to_dlq("test-topic2", 0, "Failed message", "Test failure")
        print(f"Move to DLQ response: {response}")
        
    except Exception as e:
        print(f"Test failed: {e}")
    finally:
        client.disconnect()

if __name__ == "__main__":
    test_tcp_endpoints() 