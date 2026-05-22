package com.example.centralstation.router;

import com.example.centralstation.model.WeatherReading;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.List;

/**
 * Content-Based Router for weather messages.
 * 
 * Content-Based Router Pattern: Routes messages to different channels based on content.
 * 
 * This router examines message properties and routes them accordingly:
 *   - Humidity > 70%: Route to "rain-alerts" topic (potential rain/high moisture)
 *   - All messages: Route to main processing pipeline
 *   - Failed routing: Logged but doesn't block main processing
 */
public class ContentBasedRouter {

    private static final Logger logger = LoggerFactory.getLogger(ContentBasedRouter.class);

    private final KafkaProducer<String, String> rainAlertProducer;
    private static final String RAIN_ALERTS_TOPIC = "rain-alerts";
    private static final int HUMIDITY_THRESHOLD = 70;  // percent

    public ContentBasedRouter(KafkaProducer<String, String> rainAlertProducer) {
        this.rainAlertProducer = rainAlertProducer;
    }

    /**
     * Route a weather reading based on its content.
     * Examines the message and decides if it should be sent to specialized topics.
     * 
     * @param reading The weather reading to evaluate
     * @param originalMessage The original JSON message
     * @return List of routes the message was sent to (informational)
     */
    public List<String> routeMessage(WeatherReading reading, String originalMessage) {
        List<String> routes = new java.util.ArrayList<>();

        // Content-based decision: Check humidity level
        if (reading.getWeather() != null && reading.getWeather().getHumidity() > HUMIDITY_THRESHOLD) {
            routeToRainAlerts(reading, originalMessage);
            routes.add(RAIN_ALERTS_TOPIC);
        }

        return routes;
    }

    /**
     * Route high-humidity readings to rain alerts topic.
     * These represent potential rain conditions or high moisture situations.
     * 
     * @param reading The weather reading with high humidity
     * @param originalMessage The original JSON message
     */
    private void routeToRainAlerts(WeatherReading reading, String originalMessage) {
        try {
            String key = "station-" + reading.getStationId();
            ProducerRecord<String, String> record = new ProducerRecord<>(
                    RAIN_ALERTS_TOPIC,
                    key,
                    originalMessage
            );

            rainAlertProducer.send(record, (metadata, exception) -> {
                if (exception != null) {
                    logger.warn("Failed to send rain alert for station {}: {}",
                            reading.getStationId(), exception.getMessage());
                } else {
                    logger.debug("Rain alert routed: station={}, humidity={}, offset={}",
                            reading.getStationId(),
                            reading.getWeather().getHumidity(),
                            metadata.offset());
                }
            });
        } catch (Exception e) {
            logger.error("Error routing rain alert for station {}: {}",
                    reading.getStationId(), e.getMessage(), e);
        }
    }

    /**
     * Check if a reading qualifies for rain alert routing.
     * 
     * @param reading The weather reading to check
     * @return true if humidity > HUMIDITY_THRESHOLD
     */
    public boolean isRainAlert(WeatherReading reading) {
        return reading.getWeather() != null && 
               reading.getWeather().getHumidity() > HUMIDITY_THRESHOLD;
    }

    /**
     * Get the humidity threshold for rain alerts.
     * 
     * @return Humidity percentage threshold
     */
    public static int getHumidityThreshold() {
        return HUMIDITY_THRESHOLD;
    }

    /**
     * Get the rain alerts topic name.
     * 
     * @return Topic name
     */
    public static String getRainAlertsTopic() {
        return RAIN_ALERTS_TOPIC;
    }
}
