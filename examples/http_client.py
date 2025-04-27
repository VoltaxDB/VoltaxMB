import requests
import json
import time
from typing import Dict, Any, Optional

class VoltaxMBHTTPClient:
    def __init__(self, host: str = 'localhost', port: int = 8080):
        self.base_url = f"http://{host}:{port}"
        self.session = requests.Session()

    def create_topic(self, name: str, num_partitions: int) -> Dict[str, Any]:
        """Create a new topic."""
        url = f"{self.base_url}/api/topics"
        payload = {
            "name": name,
            "num_partitions": num_partitions
        }
        response = self.session.post(url, json=payload)
        response.raise_for_status()
        return response.json() if response.content else {}

    def publish(self, topic: str, partition: int, data: str, priority: int = 1) -> Dict[str, Any]:
        """Publish a message to a topic."""
        url = f"{self.base_url}/api/publish"
        payload = {
            "topic": topic,
            "partition": partition,
            "data": data,
            "priority": priority
        }
        response = self.session.post(url, json=payload)
        response.raise_for_status()
        return response.json() if response.content else {}

    def consume(self, topic: str, partition: int) -> Dict[str, Any]:
        """Consume a message from a topic."""
        url = f"{self.base_url}/api/consume"
        params = {
            "topic": topic,
            "partition": partition
        }
        response = self.session.get(url, params=params)
        response.raise_for_status()
        return response.json() if response.content else {}

    def acknowledge(self, message_id: str, topic: str, partition: int) -> Dict[str, Any]:
        """Acknowledge a message."""
        url = f"{self.base_url}/api/ack"
        payload = {
            "message_id": message_id,
            "topic": topic,
            "partition": partition
        }
        response = self.session.post(url, json=payload)
        response.raise_for_status()
        return response.json() if response.content else {}

    def schedule_message(self, topic: str, partition: int, data: str, delay_ms: int) -> Dict[str, Any]:
        """Schedule a message for future delivery."""
        url = f"{self.base_url}/topics/{topic}/schedule"
        payload = {
            "data": data,
            "partition": partition,
            "delay": delay_ms
        }
        response = self.session.post(url, json=payload)
        response.raise_for_status()
        return response.json() if response.content else {}

    def move_to_dlq(self, topic: str, partition: int, data: str, reason: str) -> Dict[str, Any]:
        """Move a message to Dead Letter Queue."""
        url = f"{self.base_url}/topics/{topic}/dlq"
        payload = {
            "data": data,
            "partition": partition,
            "reason": reason
        }
        response = self.session.post(url, json=payload)
        response.raise_for_status()
        return response.json() if response.content else {}

def test_http_endpoints():
    """Test all HTTP endpoints."""
    client = VoltaxMBHTTPClient()
    
    try:
        # Test topic creation
        print("\nTesting topic creation...")
        response = client.create_topic("test-topic-http", 3)
        print(f"Create topic response: {response}")
        
        # Test message publishing
        print("\nTesting message publishing...")
        response = client.publish("test-topic-http", 0, "Hello, World!", 1)
        print(f"Publish response: {response}")
        
        # Test message consumption
        print("\nTesting message consumption...")
        response = client.consume("test-topic-http", 0)
        print(f"Consume response: {response}")
        
        # Test message acknowledgment
        if response and 'id' in response:
            print("\nTesting message acknowledgment...")
            ack_response = client.acknowledge(response['id'], "test-topic-http", 0)
            print(f"Acknowledge response: {ack_response}")
        
        # Test scheduled message
        print("\nTesting scheduled message...")
        response = client.schedule_message("test-topic-http", 0, "Delayed message", 5000)
        print(f"Schedule message response: {response}")
        
        # Wait for scheduled message
        print("\nWaiting for scheduled message...")
        time.sleep(6)
        response = client.consume("test-topic-http", 0)
        print(f"Consume scheduled message response: {response}")
        
        # Test DLQ
        print("\nTesting Dead Letter Queue...")
        response = client.move_to_dlq("test-topic-http", 0, "Failed message", "Test failure")
        print(f"Move to DLQ response: {response}")
        
    except requests.exceptions.RequestException as e:
        print(f"Test failed: {e}")

if __name__ == "__main__":
    test_http_endpoints() 