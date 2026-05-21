# Central Station Service

**Member 2 – Weather Station Monitoring System**

Consumes all weather readings from Kafka, archives them to partitioned **Parquet** files, and runs a **Kafka Streams** pipeline that detects rain conditions (humidity > 70 %) and publishes alerts to the `raining-alerts` topic.

---

## Project Structure

```
CentralStation/
├── src/main/java/com/example/centralstation/
│   ├── CentralStationApplication.java   ← Main entry point
│   ├── config/
│   │   └── AppConfig.java               ← All config (env vars)
│   ├── consumer/
│   │   └── WeatherKafkaConsumer.java    ← Kafka consumer (Task A)
│   ├── parquet/
│   │   ├── ParquetBatchManager.java     ← Batch buffer & flush (Task B)
│   │   └── ParquetWriterUtil.java       ← Parquet file writer (Task B)
│   ├── streams/
│   │   └── RainDetectionStream.java     ← Kafka Streams DSL (Task C)
│   ├── model/
│   │   ├── WeatherReading.java          ← Input message model
│   │   └── RainAlert.java               ← Output alert model
│   └── dummy/
│       └── DummyWeatherProducer.java    ← Local testing only (Task F)
├── src/main/resources/
│   ├── application.properties
│   └── logback.xml
├── Dockerfile                           ← Multi-stage Docker build (Task D)
└── pom.xml
```

---

## Prerequisites

| Tool      | Version |
|-----------|---------|
| Java      | 17+     |
| Maven     | 3.9+    |
| Docker    | 24+     |
| Docker Compose | v2 |

---

## Environment Variables

| Variable                  | Default                      | Description                                  |
|---------------------------|------------------------------|----------------------------------------------|
| `KAFKA_BOOTSTRAP_SERVERS` | `localhost:9092`             | Kafka broker address                         |
| `WEATHER_TOPIC`           | `weather-readings`           | Topic to consume from                        |
| `RAIN_ALERT_TOPIC`        | `raining-alerts`             | Topic to publish rain alerts to              |
| `CONSUMER_GROUP_ID`       | `central-station-archiver`   | Kafka consumer group ID                      |
| `STREAMS_APP_ID`          | `central-station-rain-detector` | Kafka Streams application ID              |
| `PARQUET_OUTPUT_PATH`     | `/app/data/parquet`          | Root dir for Parquet files                   |
| `PARQUET_BATCH_SIZE`      | `10000`                      | Records per Parquet file                     |
| `RAIN_HUMIDITY_THRESHOLD` | `70`                         | Humidity % above which RAIN_DETECTED fires   |
| `DUMMY_MODE`              | `false`                      | `true` starts built-in dummy producer        |

---

## Build

```bash
cd CentralStation
mvn package -DskipTests
```

The fat-JAR is produced at `target/CentralStation-1.0-SNAPSHOT.jar`.

---

## Running Locally (without Docker)

### 1. Start Kafka

```bash
# From DataIntensiveProject directory (Member 1)
docker compose up -d zookeeper kafka
```

### 2. Create topics

```bash
docker exec kafka kafka-topics \
  --bootstrap-server localhost:9092 \
  --create --if-not-exists \
  --topic weather-readings \
  --partitions 10 --replication-factor 1

docker exec kafka kafka-topics \
  --bootstrap-server localhost:9092 \
  --create --if-not-exists \
  --topic raining-alerts \
  --partitions 3 --replication-factor 1
```

### 3. Run Central Station with dummy data

```bash
export KAFKA_BOOTSTRAP_SERVERS=localhost:9092
export PARQUET_OUTPUT_PATH=./output/parquet
export DUMMY_MODE=true

java -jar target/CentralStation-1.0-SNAPSHOT.jar
```

---

## Running with Docker Compose (Full Stack)

```bash
# From the project root (Weather-Station-Monitoring/)

# Build Member 1's image first
cd DataIntensiveProject
docker build -t weather-station:latest .
cd ..

# Build & start everything
docker compose -f docker-compose-full.yml up --build
```

To stop cleanly:

```bash
docker compose -f docker-compose-full.yml down -v
```

---

## Parquet Output Structure

```
/app/data/parquet/
└── year=2026/
    └── month=05/
        └── day=19/
            └── station_id=3/
                └── part-00001.parquet
```

Each Parquet file contains **10,000 records** with these columns:

| Column             | Type   | Description                              |
|--------------------|--------|------------------------------------------|
| `station_id`       | long   | Weather station identifier               |
| `s_no`             | long   | Message sequence number                  |
| `battery_status`   | string | `low` / `medium` / `high`               |
| `status_timestamp` | long   | Unix epoch seconds                       |
| `humidity`         | int    | Relative humidity %                      |
| `temperature`      | int    | Temperature (unit from source)           |
| `wind_speed`       | int    | Wind speed (unit from source)            |
| `year`             | int    | Partition helper                         |
| `month`            | int    | Partition helper                         |
| `day`              | int    | Partition helper                         |

---

## Rain Alert Message Example

Published to `raining-alerts` when `humidity > 70`:

```json
{
  "station_id": 3,
  "alert_type": "RAIN_DETECTED",
  "humidity": 82,
  "temperature": 26,
  "wind_speed": 12,
  "status_timestamp": 1681521350
}
```

---

## Kafka Topic Commands Reference

```bash
# List topics
docker exec kafka kafka-topics --bootstrap-server kafka:9092 --list

# Consume rain alerts in real-time
docker exec kafka kafka-console-consumer \
  --bootstrap-server kafka:9092 \
  --topic raining-alerts \
  --from-beginning

# Check consumer group lag
docker exec kafka kafka-consumer-groups \
  --bootstrap-server kafka:9092 \
  --describe \
  --group central-station-archiver
```

---

## Integration Notes (for Member 4)

The shared Docker volume `weather-data` is mounted at `/app/data` in the `central-station` container. Member 4's service should mount the same named volume to access the Parquet files:

```yaml
# In Member 4's compose section:
volumes:
  - weather-data:/your/mount/path
```

And declare the volume as external:

```yaml
volumes:
  weather-data:
    external: true
```
