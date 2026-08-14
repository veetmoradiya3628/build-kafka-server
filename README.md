# Custom Kafka Broker (Go)

A lightweight, functional implementation of a Kafka broker written in Go. This project implements the core Kafka wire protocol, supporting cluster metadata parsing, message fetching, and message producing directly to disk using the Kafka RecordBatch format.

## 🚀 Overview

This broker supports a subset of the Kafka protocol, specifically targeting modern flexible/compact API versions (KRaft mode). It handles TCP connections, dynamically decodes variable-length request buffers, routes API calls, and reads/writes binary log files.

## ✨ Features Implemented

### 1. Protocol Routing & Wire Format

* **Message Framing:** Wraps all outgoing payloads with standard 4-byte message size integers.


* **Header Parsing:** Parses standard Kafka Request Headers (v2) to extract `APIKey`, `APIVersion`, and `CorrelationID`.


* **Compact Types:** Safely decodes and encodes Kafka's compact arrays and strings using Unsigned Varints (`Uvarint`), protecting against buffer overflows and truncated requests.

### 2. API Versions (API Key 18, v4)

Responds to clients with the broker's supported capabilities. The broker actively advertises support for:

* **Produce API:** (Key `0`, Max Version `11`)
* **Fetch API:** (Key `1`, Max Version `16`)
* **ApiVersions:** (Key `18`, Max Version `4`)
* **DescribeTopicPartitions:** (Key `75`, Max Version `0`)

### 3. Describe Topic Partitions (API Key 75, v0)

* Parses requested topic names and cross-references them against the broker's internal metadata state.


* Returns detailed partition data, including the 16-byte Topic UUID, Leader ID, and Replica Nodes.


* Returns `UNKNOWN_TOPIC_OR_PARTITION` (Error Code `3`) for non-existent topics.



### 4. Fetch API (API Key 1, v16)

Allows clients to consume messages from specific partitions.

* **Empty Requests:** Handles requests with 0 topics safely without crashing.
* **Unknown Topics:** Validates the requested 16-byte `TopicID` against metadata and returns `UNKNOWN_TOPIC_ID` (Error Code `100`) if not found.
* **Empty Partitions:** Returns a valid success response (`NO_ERROR`, Error Code `0`) with a null/0-length `COMPACT_RECORDS` payload if the partition exists but has no messages.
* **Reading from Disk:** Locates the physical `.log` file for the requested topic and partition, reads the binary `RecordBatch` from disk, and serves it to the client via `Uvarint(length + 1)` encoding.

### 5. Produce API (API Key 0, v11)

Allows clients to publish messages to the broker.

* **Validation:** Rejects requests with `UNKNOWN_TOPIC_OR_PARTITION` (Error Code `3`) if the topic or partition index does not exist in the cluster metadata.
* **Batch Parsing:** Safely parses incoming `COMPACT_RECORDS` arrays containing raw `RecordBatch` bytes.
* **Multi-Record Support:** Dynamically tracks the `BaseOffset` across multiple produce requests.
* **Binary Mutation:** Mutates the first 8 bytes of the incoming `RecordBatch` to apply the correct `BaseOffset` and extracts the `LastOffsetDelta` (bytes 23-26) to accurately increment the offset tracker for future messages.
* **Writing to Disk:** Appends valid RecordBatches directly to the partition's log file (`os.O_APPEND`).

---

## 📂 Storage & Data Mechanics

The broker acts as a KRaft (Kafka Raft) mode server, meaning it relies on a local log file rather than Zookeeper for cluster state.

### Cluster Metadata Parsing

On startup, the broker reads `/tmp/kraft-combined-logs/__cluster_metadata-0/00000000000000000000.log`.

* **Topic Records:** Extracts the Topic Name and 16-byte UUID.


* **Partition Records:** Extracts Partition IDs and maps them to their respective Topic UUIDs.
This parsed metadata acts as the source of truth for all `Produce` and `Fetch` validations.



### Topic Log Structure

When a client produces a message, it is written to disk following standard Kafka directory structures:

* **Path Format:** `/tmp/kraft-combined-logs/<topic_name>-<partition_index>/00000000000000000000.log`
* **Format:** Raw binary `RecordBatch` bytes.

---

## 🛠️ Code Structure

* **`main.go`**: Handles TCP listener binding on port `9092` and manages concurrent client connections.


* **`broker/router.go`**: The central nervous system. Routes decoded API keys to their respective handlers, manages global states (like `partitionOffsets`), and interacts with the file system.


* **`storage/metadata.go`**: Contains the KRaft log parser and `ReadVarint` zigzag decoder to build the initial cluster state.


* **`protocol/`**: A dedicated package for parsing incoming raw byte buffers and encoding strictly typed structs back into Kafka-compliant wire formats (contains `apiversions.go`, `fetch.go`, `produce.go`, etc.).



---

## 💻 How to Run

1. Ensure you have Go installed.
2. Build and run the server:
```bash
go build -o kafka-broker ./app
./kafka-broker /tmp/server.properties

```


3. Connect a Kafka client (like `kcat` or standard Kafka CLI tools) to `localhost:9092`.
