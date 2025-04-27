# VoltaxMB

VoltaxMB is a high-performance message broker written in Go, designed for scalability and reliability.

## Configuration

VoltaxMB uses a YAML configuration file to manage its settings. By default, it looks for a `config.yaml` file in the current directory, but you can specify a different path using the `--config` flag.

To get started, copy the example configuration file:

```bash
cp config.example.yaml config.yaml
```

### Configuration Options

#### Server Settings
- `host`: Host address to bind to (default: "0.0.0.0")
- `port`: Port to listen on (default: 9090)
- `max_connections`: Maximum number of concurrent connections (default: 1000)
- `read_timeout`: Read timeout in seconds (default: 30)
- `write_timeout`: Write timeout in seconds (default: 30)

#### Storage Settings
- `data_dir`: Directory to store message data (default: "./data")
- `enable_aof`: Enable Append-Only File for durability (default: true)
- `aof_sync_interval`: AOF sync interval in milliseconds (default: 1000)
- `snapshot_interval`: Snapshot interval in seconds (default: 3600)

#### Message Settings
- `max_message_size`: Maximum message size in bytes (default: 1MB)
- `retention_time`: Message retention time in seconds (default: 7 days)
- `max_partitions`: Maximum number of partitions per topic (default: 32)
- `replica_factor`: Number of replicas for each partition (default: 1)

#### Security Settings
- `enable_auth`: Enable authentication (default: false)
- `jwt_secret`: JWT secret for authentication (required if enable_auth is true)
- `tls_enabled`: Enable TLS (default: false)
- `tls_cert_file`: Path to TLS certificate file
- `tls_key_file`: Path to TLS private key file

#### Metrics Settings
- `enable_metrics`: Enable Prometheus metrics (default: true)
- `metrics_port`: Port for metrics endpoint (default: 9091)

## Running VoltaxMB

To start VoltaxMB with a custom configuration file:

```bash
voltaxmb --config /path/to/config.yaml
```

## Security Considerations

1. When enabling authentication (`enable_auth: true`), make sure to set a strong `jwt_secret`.
2. For production deployments, it's recommended to enable TLS by setting `tls_enabled: true` and providing valid certificate files.
3. Configure appropriate `max_connections` and `max_message_size` values based on your system resources.
4. The `data_dir` should be on a reliable storage device with sufficient space.

## Monitoring

When `enable_metrics` is true, VoltaxMB exposes Prometheus metrics at `http://<host>:<metrics_port>/metrics`. These metrics include:

- Connection statistics
- Message throughput
- Storage metrics
- System resource usage

## Features

- High-performance message broker
- Support for multiple publishers and consumers
- Topic-based message routing
- Message persistence and durability
- Priority queues and scheduled messages
- Dead-letter queues for failed message handling
- Prometheus metrics and health checks
- Secure authentication and authorization

## Getting Started

### Prerequisites

- Go 1.21 or later
- Make (optional, for using Makefile commands)

### Installation

```bash
# Clone the repository
git clone https://github.com/voltaxmq/voltaxmq.git
cd voltaxmq

# Install dependencies
go mod download

# Build the project
go build -o vtxmq ./cmd/broker
```

### Running the Broker

```bash
# Start the broker with default configuration
./vtxmq

# Start with custom configuration
./vtxmq --config path/to/voltaxmq.json
```

## Project Structure

```
voltaxmq/
├── cmd/                    # Command-line applications
│   ├── broker/            # TCP/HTTP server entry
│   └── cli/               # CLI client tool (vtxmq-cli)
├── config/                # Configuration management
├── internal/              # Internal packages
│   ├── broker/            # Core broker logic
│   ├── queue/             # Queue management
│   ├── persistence/       # Message persistence
│   └── utils/             # Helper functions
├── pkg/                   # Public packages
└── tests/                 # Test files
```

## Contributing

Please read [CONTRIBUTING.md](CONTRIBUTING.md) for details on our code of conduct and the process for submitting pull requests.

## License

This project is licensed under the MIT License - see the [LICENSE](LICENSE) file for details. 