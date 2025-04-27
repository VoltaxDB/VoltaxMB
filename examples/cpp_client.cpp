#include <iostream>
#include <string>
#include <vector>
#include <memory>
#include <thread>
#include <mutex>
#include <chrono>
#include <cstring>
#include <sys/socket.h>
#include <netinet/in.h>
#include <arpa/inet.h>
#include <unistd.h>
#include <nlohmann/json.hpp>

using json = nlohmann::json;

class VoltaxMBClient {
private:
    int sock;
    std::string host;
    int port;
    bool connected;
    std::mutex mutex;

    json sendFrame(uint8_t cmd, const json& payload) {
        if (!connected) {
            throw std::runtime_error("Not connected to broker");
        }

        std::lock_guard<std::mutex> lock(mutex);

        // Marshal payload
        std::string data = payload.dump();
        uint32_t length = data.length();

        // Send command
        if (send(sock, &cmd, 1, 0) != 1) {
            throw std::runtime_error("Failed to send command");
        }

        // Send length (network byte order)
        uint32_t netLength = htonl(length);
        if (send(sock, &netLength, sizeof(netLength), 0) != sizeof(netLength)) {
            throw std::runtime_error("Failed to send length");
        }

        // Send payload
        if (send(sock, data.c_str(), length, 0) != length) {
            throw std::runtime_error("Failed to send payload");
        }

        // Read response length
        uint32_t responseLength;
        if (recv(sock, &responseLength, sizeof(responseLength), 0) != sizeof(responseLength)) {
            throw std::runtime_error("Failed to read response length");
        }
        responseLength = ntohl(responseLength);

        // Read response payload
        std::vector<char> buffer(responseLength + 1);
        size_t totalRead = 0;
        while (totalRead < responseLength) {
            ssize_t bytesRead = recv(sock, buffer.data() + totalRead, responseLength - totalRead, 0);
            if (bytesRead <= 0) {
                throw std::runtime_error("Failed to receive response");
            }
            totalRead += bytesRead;
        }
        buffer[responseLength] = '\0';

        // Parse response
        json response = json::parse(buffer.data());
        if (response.contains("error")) {
            throw std::runtime_error(response["error"].get<std::string>());
        }
        return response;
    }

public:
    VoltaxMBClient(const std::string& host = "localhost", int port = 9090)
        : host(host), port(port), connected(false) {}

    ~VoltaxMBClient() {
        disconnect();
    }

    void connect() {
        sock = socket(AF_INET, SOCK_STREAM, 0);
        if (sock < 0) {
            throw std::runtime_error("Failed to create socket");
        }

        struct sockaddr_in serverAddr;
        serverAddr.sin_family = AF_INET;
        serverAddr.sin_port = htons(port);
        inet_pton(AF_INET, host.c_str(), &serverAddr.sin_addr);

        if (::connect(sock, (struct sockaddr*)&serverAddr, sizeof(serverAddr)) < 0) {
            close(sock);
            throw std::runtime_error("Failed to connect to broker");
        }

        connected = true;
        std::cout << "Connected to " << host << ":" << port << std::endl;
    }

    void disconnect() {
        if (connected) {
            close(sock);
            connected = false;
            std::cout << "Disconnected from broker" << std::endl;
        }
    }

    json createTopic(const std::string& name, int numPartitions) {
        json payload = {
            {"name", name},
            {"num_partitions", numPartitions}
        };
        return sendFrame(5, payload);
    }

    json publish(const std::string& topic, int partition, const std::string& data, int priority = 1) {
        json payload = {
            {"topic", topic},
            {"partition", partition},
            {"data", data},
            {"priority", priority}
        };
        return sendFrame(2, payload);
    }

    json consume(const std::string& topic, int partition) {
        json payload = {
            {"topic", topic},
            {"partition", partition}
        };
        return sendFrame(3, payload);
    }

    json acknowledge(const std::string& messageId, const std::string& topic, int partition) {
        json payload = {
            {"message_id", messageId},
            {"topic", topic},
            {"partition", partition}
        };
        return sendFrame(4, payload);
    }

    json scheduleMessage(const std::string& topic, int partition, const std::string& data, int delayMs) {
        json payload = {
            {"topic", topic},
            {"partition", partition},
            {"data", data},
            {"delay", delayMs}
        };
        return sendFrame(6, payload);
    }

    json moveToDLQ(const std::string& topic, int partition, const std::string& data, const std::string& reason) {
        json payload = {
            {"topic", topic},
            {"partition", partition},
            {"data", data},
            {"reason", reason}
        };
        return sendFrame(7, payload);
    }
};

void testTcpEndpoints() {
    VoltaxMBClient client;
    
    try {
        // Connect to broker
        client.connect();
        
        // Test topic creation
        std::cout << "\nTesting topic creation..." << std::endl;
        json response = client.createTopic("test-topic", 3);
        std::cout << "Create topic response: " << response.dump() << std::endl;
        
        // Test message publishing
        std::cout << "\nTesting message publishing..." << std::endl;
        response = client.publish("test-topic", 0, "Hello, World!", 1);
        std::cout << "Publish response: " << response.dump() << std::endl;
        
        // Test message consumption
        std::cout << "\nTesting message consumption..." << std::endl;
        response = client.consume("test-topic", 0);
        std::cout << "Consume response: " << response.dump() << std::endl;
        
        // Test message acknowledgment
        if (response.contains("id")) {
            std::cout << "\nTesting message acknowledgment..." << std::endl;
            json ack_response = client.acknowledge(response["id"], "test-topic", 0);
            std::cout << "Acknowledge response: " << ack_response.dump() << std::endl;
        }
        
        // Test scheduled message
        std::cout << "\nTesting scheduled message..." << std::endl;
        response = client.scheduleMessage("test-topic", 0, "Delayed message", 5000);
        std::cout << "Schedule message response: " << response.dump() << std::endl;
        
        // Wait for scheduled message
        std::cout << "\nWaiting for scheduled message..." << std::endl;
        std::this_thread::sleep_for(std::chrono::seconds(6));
        response = client.consume("test-topic", 0);
        std::cout << "Consume scheduled message response: " << response.dump() << std::endl;
        
        // Test DLQ
        std::cout << "\nTesting Dead Letter Queue..." << std::endl;
        response = client.moveToDLQ("test-topic", 0, "Failed message", "Test failure");
        std::cout << "Move to DLQ response: " << response.dump() << std::endl;
        
    } catch (const std::exception& e) {
        std::cerr << "Test failed: " << e.what() << std::endl;
    }
}

int main() {
    testTcpEndpoints();
    return 0;
} 