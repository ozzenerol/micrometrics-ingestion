# micrometrics-ingestion

![Java](https://img.shields.io/badge/Java-21-orange?logo=openjdk&logoColor=white)
![Maven](https://img.shields.io/badge/Maven-3.8+-blue?logo=apachemaven&logoColor=white)
![Cassandra](https://img.shields.io/badge/Cassandra-4.x%2F5.x-1287B1?logo=apachecassandra&logoColor=white)
![License](https://img.shields.io/badge/License-MIT-green)

High-throughput TCP ingestion server for IoT sensor telemetry, written in Java 21. Devices connect over persistent raw TCP connections, authenticate with an API key, and stream binary-encoded sensor frames that are batched and written to spool files for downstream processing.

Benchmarked at **~444,000 frames/sec sustained** end-to-end (TCP receive + spool write), with peaks above **847,000 frames/sec**, from 1,000 concurrent connections on a single machine.

---

## Tech Stack

| Layer | Technology |
|---|---|
| Runtime | Java 21 (virtual threads — JEP 444) |
| I/O concurrency | Platform threads for spool writers, virtual threads for TCP handlers |
| Device registry | Apache Cassandra 4.x / 5.x via DataStax Java Driver 4.17.0 |
| Wire format | Fixed-width binary, big-endian, no framing library |
| Config | JSON via Jackson Databind 2.17.2 |
| Logging | SLF4J 2.0.13 + Logback 1.5.6 |
| Build | Maven 3.8+ |

---

## Architecture

```
IoT Devices ──TCP──▶ IngestionServer
                           │
                    (virtual thread per connection)
                           │
                     authenticate once
                     per TCP connection
                           │
                    DeviceCache.getByApiKey()   ←──── ConcurrentHashMap (30s TTL)
                     (O(1) map lookup)                        │
                           │                         Cassandra sync thread
                           ▼
                    LinkedBlockingQueue
                           │
              ┌────────────┴──────────────┐
              ▼            ▼              ▼       (N = CPU cores, platform threads)
        spool-writer-0  spool-writer-1  ...
              │
        ByteBuffer.allocateDirect(MAX_BATCH × 22)
        FileChannel.open(CREATE_NEW)
        FileChannel.write(buffer)          ← single syscall per batch
        Files.move(.bin → .bin.done)       ← atomic rename signals completion
```

**IngestionServer** accepts TCP connections and dispatches each to a Java 21 virtual thread. On the first frame it validates the device's API key; subsequent frames on the same connection bypass authentication entirely. Validated frames are dropped into a shared `LinkedBlockingQueue`.

**SpoolWriter** runs `N` platform threads (one per CPU core). Each thread blocks on the queue, accumulates frames into a batch (10k–100k frames, or up to 200ms), serializes the batch into a thread-local direct `ByteBuffer`, and flushes it to disk in a single `FileChannel.write()` call. The file is then atomically renamed from `.bin` to `.bin.done`.

Platform threads are used instead of virtual threads for spool writers because `FileChannel.write()` invokes a blocking native syscall, which pins a virtual thread to its carrier thread — eliminating the multiplexing benefit and potentially starving other virtual threads.

**DeviceCache** holds all active devices in a `ConcurrentHashMap`, refreshed from Cassandra every 30 seconds. Lookup cost per connection is O(1). On a cache miss, the server falls back to a direct Cassandra query and logs the miss.

**Crash recovery:** On startup, `SpoolWriter` scans the spool directory, deletes any orphaned `.bin` files (written but never renamed — i.e. incomplete), and initialises the sequence counter to `max(existing .bin.done sequence) + 1`, guaranteeing no filename collisions across restarts.

---

## Wire Protocol

Frames are binary, big-endian, encoded with Java's `DataOutputStream` conventions. Frames are written back-to-back on the same TCP connection with no envelope or delimiter; the server reads until the connection closes.

| Field        | Type                          | Size                          |
|--------------|-------------------------------|-------------------------------|
| `apiKey`     | UTF-8 string                  | 2-byte length prefix + N bytes |
| `deviceId`   | signed 64-bit integer         | 8 bytes                       |
| `timestamp`  | Unix epoch milliseconds (i64) | 8 bytes                       |
| `metricType` | signed 16-bit integer         | 2 bytes                       |
| `value`      | 32-bit IEEE 754 float         | 4 bytes                       |

The API key is only validated on the first frame of each TCP connection. All subsequent frames on the same connection are trusted — the key is still encoded in the payload but not re-checked, which eliminates per-frame Cassandra or cache overhead at scale.

---

## Spool File Format

Each `.bin.done` file contains a contiguous sequence of fixed-size records with no file header. The API key is stripped at ingestion time; device identity is carried by `deviceId`.

| Field        | Type                          | Size    | Offset |
|--------------|-------------------------------|---------|--------|
| `deviceId`   | signed 64-bit integer         | 8 bytes | 0      |
| `timestamp`  | Unix epoch milliseconds (i64) | 8 bytes | 8      |
| `metricType` | signed 16-bit integer         | 2 bytes | 16     |
| `value`      | 32-bit IEEE 754 float         | 4 bytes | 18     |

**Record size: 22 bytes.** File size = `N × 22` bytes, where N is the batch size (10,000–100,000 frames under normal load).

Files are visible to downstream consumers only once renamed to `.bin.done`. Consumers should poll for `.bin.done` files and process them in sequence-number order. Any `.bin` file present in the directory at startup is an incomplete write from a previous crash and will be deleted automatically.

---

## Benchmarks

Measured on Apple M-series (16 cores), local Cassandra, 1,000 concurrent Go goroutines, 1,000,000 frames total:

```
throughput: 847,345 frames/sec   (first second — queue draining fast)
throughput: 152,624 frames/sec   (second second — tail frames)
sent 1,000,000 frames in 2.251s (444,338 frames/sec average)
```

| Metric | Value |
|---|---|
| Sustained throughput | **444,338 frames/sec** |
| Peak throughput | **847,345 frames/sec** |
| Total time (1M frames) | **2.25 seconds** |
| Spool write latency (10k-frame batch) | **0–1ms** |
| Spool write latency (18k-frame batch) | **4–6ms** |
| Disk bytes written per 10k frames | ~220 KB |
| Writer threads | 16 (= CPU cores) |
| TCP connections (load test) | 1,000 |

Spool writer throughput matches the TCP receive rate in real time — no queue build-up was observed during the test. The bottleneck is the TCP receive path, not disk I/O.

---

## Requirements

- Java 21+
- Maven 3.8+
- Apache Cassandra 4.x or 5.x

---

## Cassandra Schema

```cql
CREATE TABLE devices (
    api_key          text PRIMARY KEY,
    device_id        bigint,
    name             text,
    firmware_version text,
    registered_at    timestamp,
    last_seen_at     timestamp,
    active           boolean
);
```

The server queries this table on startup (full scan to warm the cache) and every 30 seconds thereafter. A direct point-lookup by `api_key` is performed only on cache misses.

---

## Configuration

Create `config.json` in the working directory before starting:

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

| Field | Description |
|---|---|
| `port` | TCP port the server listens on |
| `spool.directory` | Directory for spool files (must exist and be writable) |
| `database.host` | Cassandra contact point hostname |
| `database.port` | Cassandra native transport port (default: 9042) |
| `database.username` | Cassandra credentials |
| `database.password` | Cassandra credentials |
| `database.keyspace` | Keyspace containing the `devices` table |
| `database.localDataCenter` | Cassandra local datacenter name (e.g. `datacenter1`) |

---

## Build & Run

```bash
mvn package -DskipTests

java -cp target/micrometrics-ingestion-1.0-SNAPSHOT.jar \
     io.micrometrics.ingestion.IngestionService
```

The server starts the device cache sync, then begins accepting TCP connections. Spool files appear in the configured directory as `spool-<seq>.bin.done`.

---

## Load Testing

A Go client is included at `scripts/client.go`. It opens 1,000 concurrent TCP connections and sends frames as fast as possible, printing per-second throughput to stdout.

```bash
go run scripts/client.go --frames=1000000
```

The client targets `localhost:9080` using API key `test`. Before running, insert a device with that key into Cassandra:

```cql
INSERT INTO devices (api_key, device_id, name, active)
VALUES ('test', 1, 'load-test-device', true);
```

---

## Dependencies

| Library | Version | Purpose |
|---|---|---|
| DataStax Java Driver | 4.17.0 | Cassandra client |
| Jackson Databind | 2.17.2 | JSON config parsing |
| SLF4J | 2.0.13 | Logging facade |
| Logback Classic | 1.5.6 | Logging implementation |
| Lombok | 1.18.34 | Boilerplate reduction |
