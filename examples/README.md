# VoltaxMB Client Examples

This directory contains example clients for VoltaxMB in different programming languages.

## Python Clients

### TCP Client
The Python TCP client demonstrates how to use VoltaxMB's TCP protocol with Python.

#### Requirements
- Python 3.6+
- No additional dependencies required

#### Running the Example
```bash
python python_client.py
```

### HTTP Client
The Python HTTP client demonstrates how to use VoltaxMB's HTTP API with Python.

#### Requirements
- Python 3.6+
- requests library (`pip install requests`)

#### Running the Example
```bash
python http_client.py
```

## C++ Client

The C++ client demonstrates how to use VoltaxMB's TCP protocol with C++.

### Requirements
- C++17 compatible compiler
- CMake 3.10+
- nlohmann_json library

### Building the Example
```bash
mkdir build
cd build
cmake ..
make
```

### Running the Example
```bash
./voltaxmb_client
```

## Features Demonstrated

All clients demonstrate:
1. Topic creation
2. Message publishing
3. Message consumption
4. Message acknowledgment
5. Scheduled messages
6. Dead Letter Queue (DLQ)

## Testing

The examples include comprehensive tests for all endpoints. To run the tests:

1. Start the VoltaxMB broker:
```bash
./voltaxmb.exe
```

2. Run any of the clients:
```bash
# Python TCP
python python_client.py

# Python HTTP
python http_client.py

# C++
./voltaxmb_client
```

## Error Handling

All clients include proper error handling for:
- Connection failures
- Network errors
- Protocol errors
- Response parsing

## Best Practices

The examples demonstrate:
- Thread-safe operations
- Proper resource cleanup
- Error handling
- Connection management
- Response validation

## Protocol Differences

### TCP Protocol
- More efficient for high-throughput scenarios
- Lower latency
- Binary protocol
- Requires custom client implementation

### HTTP Protocol
- Easier to use
- More widely supported
- Text-based protocol
- Can use standard HTTP clients
- Better for web applications 