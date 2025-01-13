# Sieve Cache

A high-performance probabilistic cache implementation in Go with ghost cache support for intelligent eviction policies.

## Features

- Probabilistic admission control
- Ghost cache for optimized re-admission
- Configurable TTL support
- Thread-safe operations
- Memory-aware eviction
- Automatic cleanup of expired entries
- Performance metrics and statistics

## Installation

```bash
git clone https://github.com/abubakardev0/sieve-cache.git
cd sieve-cache
go mod tidy
```

## Documentation

To verify documentation:
```bash
go doc -all sieve
```

---

## Running Tests

Run the tests to verify functionality:
```bash
go test -v ./sieve
```

## Running with Docker

### Prerequisites

- Install [Docker](https://docs.docker.com/get-docker/).

### Build and Run

1. Build the Docker image:
   ```bash
   docker build -t sieve-cache .
   ```

2. Run the Docker container:
   ```bash
   docker run -p 8080:8080 sieve-cache
   ```

---

## Running Without Docker

1. Install dependencies:
   ```bash
   go mod tidy
   ```

2. Build the application:
   ```bash
   go build -o sieve-cache main.go
   ```

3. Run the application:
   ```bash
   ./sieve-cache
   ```

---