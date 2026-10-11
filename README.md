# Distributed Object Storage Service

A distributed object storage system built with **Go, gRPC, BadgerDB, and Docker**. Files are split into chunks, distributed across three storage nodes, and reconstructed using metadata stored in a persistent key-value database.

## Architecture

The system consists of:

- **CLI Client:** Uploads and downloads files.
- **Storage Nodes:** Three independent gRPC servers storing chunks on disk.
- **Metadata Server:** A gRPC server using BadgerDB to persist object and chunk metadata.
- **Docker Compose:** Runs the storage nodes and metadata server in isolated containers with persistent volumes.

### Upload Flow

1. Read a file and split it into 1 MiB chunks.
2. Calculate a SHA-256 checksum for each chunk.
3. Distribute chunks across storage nodes using round-robin placement.
4. Save chunk locations, sizes, and checksums in BadgerDB through the metadata server.

### Download Flow

1. Retrieve the object's metadata.
2. Fetch each chunk from its assigned storage node.
3. Verify each chunk's SHA-256 checksum.
4. Reconstruct the file in the original order.
5. Rename the completed temporary file to the requested output path.

## Technologies

- **Go:** Client, storage nodes, and metadata server
- **gRPC / Protocol Buffers:** Inter-service communication
- **BadgerDB:** Persistent object metadata
- **Docker Compose:** Containerized deployment
- **SHA-256:** Chunk integrity verification

## Getting Started

### Prerequisites

- Go
- Docker with Docker Compose

### Start the Servers

```bash
docker compose up --build -d
```

Verify that all four containers are running:

```bash
docker compose ps
```

### Upload a File

```bash
go run ./cmd/client upload sample.txt
```

### Download a File

```bash
go run ./cmd/client download sample.txt restored.txt
```

### Verify File Integrity

```bash
shasum -a 256 sample.txt restored.txt
```

The two checksums should match.

### Stop the Servers

```bash
docker compose down
```

Docker volumes preserve stored data unless explicitly removed.

## Testing

Run unit tests:

```bash
go test ./...
```

Run the end-to-end integration test while the Docker services are running:

```bash
go test -tags=integration -v ./internal/storage -run '^TestDistributedUploadDownload$' -count=1
```

The integration test uploads a multi-chunk object across three storage nodes, saves and retrieves metadata, downloads the chunks, and verifies the reconstructed content.

## Performance Benchmark

Concurrent chunk-upload performance was measured using three local Docker storage nodes and 1 MiB chunks.

| Concurrent Clients | Average Aggregate Throughput |
|---|---:|
| 1 | 100.72 MB/s |
| 5 | 150.15 MB/s |
| 10 | 151.18 MB/s |
| 20 | 142.64 MB/s |

**Peak observed throughput:** 162.23 MB/s at 10 concurrent clients.

Benchmark command:

```bash
go test ./internal/storage -run '^$' -bench BenchmarkConcurrentUploads -benchtime=20x -count=3 -benchmem
```

These are local measurements of direct chunk uploads, excluding metadata operations and complete file-upload overhead. They are not production performance guarantees.

## Reliability and Safety

- SHA-256 checksum verification detects corrupted downloaded chunks.
- Persistent Docker volumes preserve chunk data and metadata across container restarts.
- Chunk ID validation rejects direct path-traversal attempts.
- Temporary download files prevent incomplete downloads from replacing the requested destination.
- The CLI rejects uploads when an object with the same ID already exists.

## Current Limitations

- Each chunk is stored on one node; replication and automatic failover are not implemented.
- Storage-node addresses are statically configured.
- Duplicate-object checks are not concurrency-safe.
- Failed uploads can leave orphaned chunks.
- Uploads retain all chunks in memory before transmission.
- Authentication and TLS are not implemented.
- Filesystem symlink attacks are not fully mitigated.

## Future Improvements

- Chunk replication and automatic failover
- Streaming uploads and downloads
- Atomic object creation and upload cleanup
- Dynamic storage-node discovery
- Authentication, TLS, and operational monitoring