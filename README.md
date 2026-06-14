# micrometrics-ingestion

A high-throughput TCP ingestion server for IoT sensor data, written in Java 21. Devices connect over raw TCP, authenticate with an API key, and stream binary sensor frames that are batched and written to spool files for downstream processing.

## Architecture

```
IoT Devices ──TCP──▶ IngestionServer ──▶ SpoolWriter ──▶ spool-*.bin.done
                            │
                      DeviceCache (30s refresh)
                            │
                       Cassandra (devices table)
```

**IngestionServer** — accepts TCP connections and dispatches each to a virtual thread. Reads binary frames from the socket, validates the device's API key, and submits valid frames to the spool queue.

**SpoolWriter** — drains the frame queue in batches (10k–100k frames, or every 200 ms) and writes them to binary spool files. Files are written as `.bin` then atomically renamed to `.bin.done` when complete.

**DeviceCache** — an in-memory `ConcurrentHashMap` of all active devices, refreshed from Cassandra every 30 seconds. On a cache miss, the server falls back to a direct DB lookup.

## Wire Protocol

Frames are binary, big-endian, using Java's `DataOutputStream` encoding:

| Field       | Type              | Size           |
|-------------|-------------------|----------------|
| `apiKey`    | UTF-8 string      | 2-byte length prefix + N bytes |
| `deviceId`  | signed 64-bit int | 8 bytes        |
| `timestamp` | Unix ms (int64)   | 8 bytes        |
| `metricType`| signed 16-bit int | 2 bytes        |
| `value`     | 32-bit float      | 4 bytes        |

Frames are written back-to-back on the same TCP connection. The server reads until the connection is closed.

## Spool File Format

Each `.bin.done` spool file contains a sequence of fixed-size records (no header):

| Field       | Type              | Size    |
|-------------|-------------------|---------|
| `deviceId`  | signed 64-bit int | 8 bytes |
| `timestamp` | Unix ms (int64)   | 8 bytes |
| `metricType`| signed 16-bit int | 2 bytes |
| `value`     | 32-bit float      | 4 bytes |

Record size: **22 bytes**. The API key is stripped — device identity is encoded by `deviceId`.

## Requirements

- Java 21+
- Maven 3.8+
- Apache Cassandra (tested with 4.x / 5.x)

## Cassandra Schema

The server expects a `devices` table in the configured keyspace:

```cql
CREATE TABLE devices (
    api_key         text PRIMARY KEY,
    device_id       bigint,
    name            text,
    firmware_version text,
    registered_at   timestamp,
    last_seen_at    timestamp,
    active          boolean
);
```

## Configuration

Create a `config.json` in the working directory before starting the server:

```json
{
  "port": 9080,
  "spool": {
    "directory": "/var/spool/micrometrics",
    "maxRows": 500
  },
  "database": {
    "host": "localhost",
    "port": 9042,
    "username": "cassandra",
    "password": "cassandra",
    "keyspace": "iot",
    "localDataCenter": "datacenter1"
  }
}
```

| Field                    | Description                                      |
|--------------------------|--------------------------------------------------|
| `port`                   | TCP port the server listens on                   |
| `spool.directory`        | Directory where spool files are written          |
| `database.host`          | Cassandra contact point hostname                 |
| `database.port`          | Cassandra native transport port (default 9042)   |
| `database.username`      | Cassandra username                               |
| `database.password`      | Cassandra password                               |
| `database.keyspace`      | Keyspace containing the `devices` table          |
| `database.localDataCenter` | Cassandra local datacenter name               |

## Build & Run

```bash
mvn package -DskipTests

java -cp target/micrometrics-ingestion-1.0-SNAPSHOT.jar \
     io.micrometrics.ingestion.IngestionService
```

The server expects `config.json` to be present in the current working directory.

## Load Testing

A Go client is included at `scripts/client.go` for throughput testing. It spawns 1000 concurrent connections and sends frames as fast as possible, printing per-second throughput.

```bash
go run scripts/client.go -frames 1000000
```

The client connects to `localhost:9080` using the API key `test`. Make sure a device with that key exists and is active in Cassandra before running.

## Dependencies

| Library                          | Version | Purpose                        |
|----------------------------------|---------|--------------------------------|
| DataStax Java Driver             | 4.17.0  | Cassandra client               |
| Jackson Databind                 | 2.17.2  | JSON config parsing            |
| SLF4J + Logback                  | 2.0.13 / 1.5.6 | Logging              |
| Lombok                           | 1.18.34 | Boilerplate reduction          |
