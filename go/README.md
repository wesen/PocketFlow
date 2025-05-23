# PocketFlow Go - Redis Streams Integration

This document describes the Redis Streams integration for PocketFlow Go event-driven agent framework.

## Quick Start

### 1. Start Redis (using Docker)

```bash
# Start Redis with Docker Compose
docker-compose up -d

# Check Redis is running
docker-compose ps
```

### 2. Run PocketFlow with Redis

```bash
# Basic flow with Redis and observability
./go -flow basic -observability

# Verbose observability
./go -flow qa -observability-verbose

# Custom Redis address
./go -redis-addr redis:6379 -flow basic
```

### 3. Alternative: In-Memory Messaging

```bash
# Run without Redis (in-memory messaging)
./go -redis=false -flow basic -observability
```

## Configuration

### Command-Line Flags

| Flag | Default | Description |
|------|---------|-------------|
| `-redis` | `true` | Use Redis Streams for messaging |
| `-redis-addr` | `localhost:6379` | Redis server address |
| `-observability` | `false` | Enable observability with console output |
| `-observability-verbose` | `false` | Enable verbose observability |
| `-flow` | `basic` | Flow type to run (basic, qa, branching) |
| `-web` | `false` | Start web UI server |
| `-web-port` | `8080` | Web UI server port |

### Examples

```bash
# Run basic flow with Redis
./go -flow basic

# Run with custom Redis instance
./go -redis-addr my-redis:6379 -flow qa

# Run web UI with Redis
./go -web

# Run with in-memory messaging (no Redis required)
./go -redis=false -flow basic

# Full observability with Redis
./go -flow branching -observability-verbose
```

## Architecture

### Redis Consumer Groups

PocketFlow uses Redis Streams with consumer groups to ensure proper message isolation:

- **Main Application**: `pocketflow_main` consumer group
- **Observability System**: `pocketflow_observability` consumer group

This design ensures that:
- Observability doesn't steal events from the main application
- Multiple instances can run simultaneously
- Messages are persistent and can be replayed
- Load balancing across multiple consumers

### Message Flow

```
Application Events → Redis Streams → Consumer Groups
                                  ├── Main Application (pocketflow_main)
                                  └── Observability (pocketflow_observability)
```

### Fallback Behavior

When Redis is disabled (`-redis=false`):
- Uses in-memory Go channels for messaging
- Observability shares the same message bus with main application
- No persistence or cross-instance communication
- Suitable for development and testing

## Docker Setup

The included `docker-compose.yml` provides:

```yaml
services:
  redis:
    image: redis:7-alpine
    ports:
      - "6379:6379"
    command: redis-server --appendonly yes
    volumes:
      - redis_data:/data
    healthcheck:
      test: ["CMD", "redis-cli", "ping"]
      interval: 5s
      timeout: 3s
      retries: 5
```

### Redis Management

```bash
# Start Redis
docker-compose up -d

# Stop Redis
docker-compose down

# View Redis logs
docker-compose logs redis

# Connect to Redis CLI
docker-compose exec redis redis-cli

# Monitor Redis streams
docker-compose exec redis redis-cli MONITOR
```

## Development

### Building

```bash
go build
```

### Dependencies

The Redis integration adds these Go modules:
- `github.com/ThreeDotsLabs/watermill-redisstream`
- `github.com/redis/go-redis/v9`

### Testing

```bash
# Test with Redis
docker-compose up -d
./go -flow basic -observability

# Test without Redis
./go -redis=false -flow basic -observability
```

## Troubleshooting

### Redis Connection Issues

```bash
# Check if Redis is running
docker-compose ps

# Check Redis connectivity
docker-compose exec redis redis-cli ping

# View application logs for Redis errors
./go -flow basic 2>&1 | grep -i redis
```

### Performance Considerations

- Redis Streams provide excellent performance for most use cases
- For high-throughput scenarios, consider Redis cluster setup
- Monitor Redis memory usage with persistent streams
- Use `MAXLEN` to limit stream size if needed

### Observability Issues

If observability events are missing:
- Ensure both main and observability systems are using same Redis instance
- Check consumer group status: `XINFO GROUPS <stream_name>`
- Verify messages in streams: `XLEN <stream_name>`

## Production Deployment

### Redis Configuration

For production, consider:
- Redis cluster for high availability
- Persistent storage configuration
- Memory optimization settings
- Network security (TLS, AUTH)
- Monitoring and alerting

### Application Configuration

```bash
# Production example with external Redis
./go -redis-addr prod-redis.example.com:6379 -flow production_workflow
```

### Monitoring

The observability system provides real-time monitoring of:
- Flow execution status
- Node completion events
- Error tracking
- Performance metrics

Use the verbose mode for detailed debugging:
```bash
./go -observability-verbose -flow <flow_name>
```