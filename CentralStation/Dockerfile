# ============================================================
# Central Station — Dockerfile
# Stage 1: Build fat-JAR with Maven
# Stage 2: Run with lightweight JRE 17
# ============================================================

# ---- Stage 1: Build ----------------------------------------
FROM maven:3.9.6-eclipse-temurin-17 AS builder

WORKDIR /build

# Copy pom first so Maven dependencies are cached as a separate layer
COPY pom.xml .
RUN mvn dependency:go-offline -q

# Build the fat-JAR
COPY src ./src
RUN mvn package -DskipTests -q

# ---- Stage 2: Runtime --------------------------------------
FROM eclipse-temurin:17-jre-jammy

WORKDIR /app

# Copy the fat-JAR
COPY --from=builder /build/target/CentralStation-1.0-SNAPSHOT.jar app.jar

# Create the Parquet output directory (mounted as a Docker volume at runtime)
RUN mkdir -p /app/data/parquet

# ---- Environment Variables (override these at runtime) ----
ENV KAFKA_BOOTSTRAP_SERVERS=kafka:9092
ENV WEATHER_TOPIC=weather-readings
ENV RAIN_ALERT_TOPIC=raining-alerts
ENV PARQUET_OUTPUT_PATH=/app/data/parquet

# Declare the volume that will be shared with Member 4
VOLUME ["/app/data"]

# Start the application
ENTRYPOINT ["java", "-jar", "app.jar"]
