# VoltaxMB Test Client

A comprehensive test client for the VoltaxMB message broker system. This client helps verify the functionality of your VoltaxMB installation and can be used for both development and production testing.

## Features

- Basic topic operations (create, publish, consume)
- Priority queue testing
- Scheduled messages
- Dead letter queue (DLQ)
- Consumer groups
- Metrics monitoring
- Automatic resource cleanup
- Configurable broker and metrics endpoints

## Prerequisites

- Go 1.16 or later
- VoltaxMB broker running
- Metrics server running (optional)

## Building

To build the test client:

```bash
go build -ldflags="-s -w" -o voltaxmb-test-client.exe tests/client/main.go
```

The `-ldflags="-s -w"` flags reduce the binary size by stripping debug information.

## Usage

### Basic Usage

```bash
./voltaxmb-test-client.exe
```

This will run all tests with default settings:
- Broker host: http://localhost:8080
- Metrics host: http://localhost:9091
- Cleanup: enabled

### Custom Configuration

```bash
# Custom broker host
./voltaxmb-test-client.exe -broker=http://my-broker:8080

# Custom metrics host
./voltaxmb-test-client.exe -metrics=http://my-metrics:9091

# Disable cleanup
./voltaxmb-test-client.exe -cleanup=false

# Multiple options
./voltaxmb-test-client.exe -broker=http://my-broker:8080 -metrics=http://my-metrics:9091 -cleanup=false
```

### Running Tests

You can use the provided test scripts to run comprehensive tests:

#### Windows
```bash
test.bat
```

#### Linux/macOS
```bash
chmod +x test.sh
./test.sh
```

## Test Scenarios

The test client verifies the following scenarios:

1. **Basic Topic Operations**
   - Topic creation
   - Message publishing
   - Message consumption

2. **Priority Queue**
   - Publishing messages with different priorities
   - Verifying priority-based delivery

3. **Scheduled Messages**
   - Scheduling messages for future delivery
   - Verifying delivery timing

4. **Dead Letter Queue**
   - Publishing invalid messages
   - Verifying DLQ handling

5. **Consumer Groups**
   - Group creation
   - Group joining
   - Message distribution

6. **Metrics**
   - Verifying metrics endpoint
   - Checking basic metrics

## Error Handling

The test client includes comprehensive error handling:
- Detailed error messages
- Proper HTTP status code handling
- Response validation
- Resource cleanup on failure

## Production Use

For production use, consider the following:

1. **Security**
   - Use HTTPS for broker and metrics endpoints
   - Implement proper authentication
   - Secure sensitive data

2. **Performance**
   - Adjust timeouts for your environment
   - Consider load testing with multiple instances
   - Monitor resource usage

3. **Monitoring**
   - Enable metrics collection
   - Set up alerts for test failures
   - Log test results for analysis

## Troubleshooting

Common issues and solutions:

1. **Connection Refused**
   - Verify broker is running
   - Check firewall settings
   - Confirm correct host/port

2. **Test Failures**
   - Check broker logs
   - Verify topic permissions
   - Ensure sufficient resources

3. **Cleanup Issues**
   - Check broker permissions
   - Verify topic existence
   - Review cleanup logs

## Contributing

Contributions are welcome! Please follow these steps:

1. Fork the repository
2. Create a feature branch
3. Make your changes
4. Run the test suite
5. Submit a pull request

## License

This project is licensed under the MIT License - see the LICENSE file for details. 