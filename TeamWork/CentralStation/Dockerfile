# ============================================================
# Central Station — Dockerfile
# Stage 1: Build fat-JAR with Maven
# Stage 2: Run with lightweight JRE 17
# ============================================================

# ---- Stage 1: Build ----------------------------------------
# NOTE: Run this build from the TeamWork/ directory so both
#       CentralStation/ and Bitcask/ are inside the build context:
#
#   docker build -f CentralStation/Dockerfile -t central-station .
#
FROM maven:3.9.6-eclipse-temurin-17 AS builder

WORKDIR /build

# Copy pom first so Maven dependency layer is cached
COPY CentralStation/pom.xml .
RUN mvn dependency:go-offline -q

# Copy CentralStation source and the shared Bitcask source root.
# The relative path ../../Bitcask in pom.xml resolves to /Bitcask here.
COPY CentralStation/src ./src
COPY Bitcask ./../../Bitcask

RUN mvn package -DskipTests -q

# ---- Stage 2: Runtime --------------------------------------
FROM eclipse-temurin:17-jre-jammy

WORKDIR /app

# Copy the fat-JAR
COPY --from=builder /build/target/CentralStation-1.0-SNAPSHOT.jar app.jar

# Create output directories (mounted as a Docker volume at runtime)
RUN mkdir -p /app/data/parquet /app/data/bitcask

# ---- Environment Variables (override these at runtime) ----
ENV KAFKA_BOOTSTRAP_SERVERS=kafka:9092
ENV WEATHER_TOPIC=weather-readings
ENV RAIN_ALERT_TOPIC=raining-alerts
ENV PARQUET_OUTPUT_PATH=/app/data/parquet
ENV BITCASK_DATA_DIR=/app/data/bitcask

# Declare the volume that will be shared with Member 4
VOLUME ["/app/data"]

# Start the application
ENTRYPOINT ["java", "-jar", "app.jar"]
