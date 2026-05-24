# IoT Weather Station Monitoring --- Data Acquisition Pipeline

A scalable, high-throughput IoT data acquisition pipeline that simulates and streams real-time sensor metrics from 10 concurrent weather stations to a central **Apache Kafka** cluster. The system features a custom **Channel Adapter** pattern to integrate live meteorological data directly from the **Open-Meteo REST API**.

---

## Technology Stack & Tooling

* **Core Runtime:** Java (JDK 17), Multi-threaded executor frameworks
* **Event Streaming & Orchestration:** Apache Kafka, Apache Zookeeper
* **Integration Patterns:** Enterprise Integration Patterns (EIP) --- Channel Adapter
* **Containerization & Deployment:** Docker, Docker Compose (Zookeeper, Kafka, and 10 dynamic station instances)
* **Testing & Tools:** Maven, SLF4J, Logback logging, Jackson JSON Mapper

---

##  Architecture Overview

The system establishes a multi-threaded data acquisition layer that guarantees message reliability and zero-duplicate streaming across distributed nodes.

```
+--------------------------------------------------------------+
|                    Data Acquisition Layer                    |
|                                                              |
|  +-------------------+              +-------------------+    |
|  | Weather Station 1 |              | Weather Station 2 |    |
|  | (Simulated Mode)  |              | (Open-Meteo Mode) |    |
|  +---------+---------+              +---------+---------+    |
|            |                                  |              |
|            +-----------------+----------------+              |
|                              |                               |
|                              v                               |
|                     [ Java Thread Pool ]                     |
+------------------------------+-------------------------------+
                               |
                               | (Stateless JSON Payload)
                               v
               +-------------------------------+
               |   Apache Kafka Ingestion      |
               |   - bootstrap: kafka:9092     |
               |   - topic: weather-readings   |
               |   - partitions: 10            |
               +---------------+---------------+
                               |
                               v
               +-------------------------------+
               |    Apache Zookeeper Cluster   |
               +-------------------------------+
```

### Key Architectural Configurations

1. **Thread Pool Concurrency:** Dynamic thread executor pools (`ExecutorService`) spawn independent, non-blocking runners to handle high ingestion frequency from multiple sensors concurrently.
2. **Idempotence & Reliability:**
   * `enable.idempotence=true` guarantees exactly-once delivery semantics under network packet loss.
   * `acks=all` ensures replication acknowledgments across the cluster brokers before committing writes.
   * `compression.type=snappy` reduces network overhead for streaming large IoT payloads.
3. **Simulated Packet Loss:** Implements a realistic 10% packet drop rate that increments message sequence numbers without transmitting them, allowing downstream visualization tools (like Kibana) to detect transmission gaps easily.

---

##  API & JSON Schema Mappings

Weather stations publish a highly structured JSON schema to the `weather-readings` topic:

```json
{
  "station_id": 1,
  "s_no": 452,
  "battery_status": "medium",
  "status_timestamp": 1782298642,
  "weather": {
    "humidity": 65,
    "temperature": 78,
    "wind_speed": 42
  }
}
```

### Data Fields

| JSON Key | Type | Description |
| :--- | :--- | :--- |
| `station_id` | `long` | Unique identifier for the specific IoT node (1--10). |
| `s_no` | `long` | Auto-incrementing sequence counter. |
| `battery_status` | `string` | Node energy status distribution (`low`: 30%, `medium`: 40%, `high`: 30%). |
| `status_timestamp` | `long` | Epoch timestamp of read execution. |
| `weather.humidity` | `int` | Relative humidity percentage (0--100%). |
| `weather.temperature` | `int` | Sensor temperature range (30--120°F). |
| `weather.wind_speed` | `int` | Anemometer reading range (0--150 km/h). |

---

##  Quick Start & Deployment

### Prerequisites
* Docker & Docker Compose
* Maven (optional, only for building jar outside container)

### Setup & Launch

1. **Configure Simulation Mode (Optional):**
   The application defaults to reading live data from Open-Meteo. To toggle the mock generation engine, set:
   ```bash
   export STATION_TYPE="simulated"
   ```

2. **Start the Infrastructure Stack:**
   Launch Zookeeper, the Kafka Broker cluster, and the 10 concurrent stations:
   ```bash
   docker-compose up --build
   ```

3. **Verify Logging Output:**
   Observe active sensor logs:
   ```bash
   docker logs -f station-1
   ```
   *Expect output showing successful ingestion offsets, partition allocations, and JSON payloads:*
   `[INFO] Station 1 -> partition=3 offset=145 s_no=42 reading={"station_id":1,"s_no":42,...}`
