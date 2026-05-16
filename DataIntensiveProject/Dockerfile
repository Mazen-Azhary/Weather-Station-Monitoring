FROM maven:3.9.6-eclipse-temurin-17 AS builder

WORKDIR /build

COPY pom.xml .
RUN mvn dependency:go-offline -q

COPY src ./src
RUN mvn clean package -DskipTests -q

FROM eclipse-temurin:17-jre-jammy

LABEL maintainer="Weather Monitoring Project"
LABEL description="IoT Weather Station – publishes readings to Kafka"

WORKDIR /app

COPY --from=builder /build/target/DataIntensiveProject-1.0-SNAPSHOT.jar app.jar

ENV STATION_ID=1
ENV KAFKA_BOOTSTRAP=kafka:9092

HEALTHCHECK --interval=30s --timeout=10s --start-period=15s --retries=3 \
    CMD pgrep -f "app.jar" > /dev/null || exit 1

ENTRYPOINT ["java", "-jar", "app.jar"]