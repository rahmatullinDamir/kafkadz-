package org.example.kafkadz;

import org.springframework.stereotype.Component;
import java.time.LocalDate;
import java.util.Arrays;
import java.util.List;
import java.util.Random;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.kafka.core.KafkaTemplate;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.scheduling.annotation.Scheduled;
import com.fasterxml.jackson.datatype.jsr310.JavaTimeModule;
import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;

@Component
public class WeatherProducer {
    private static final List<String> CITIES = Arrays.asList("Магадан", "Чукотка", "Питер", "Тюмень", "Москва");
    private static final List<String> CONDITIONS = Arrays.asList("солнечно", "облачно", "дождь");
    private static final Random RANDOM = new Random();

    @Value("${weather.topic}")
    private String topic;

    private final KafkaTemplate<String, String> kafkaTemplate;
    private final ObjectMapper objectMapper = new ObjectMapper().registerModule(new JavaTimeModule());

    @Autowired
    public WeatherProducer(KafkaTemplate<String, String> kafkaTemplate) {
        this.kafkaTemplate = kafkaTemplate;
    }

    public WeatherData generateRandomWeather() {
        String city = CITIES.get(RANDOM.nextInt(CITIES.size()));
        String condition = CONDITIONS.get(RANDOM.nextInt(CONDITIONS.size()));
        int temperature = RANDOM.nextInt(36);
        LocalDate date = LocalDate.now().minusDays(RANDOM.nextInt(7));
        return new WeatherData(city, condition, temperature, date);
    }

    public void sendRandomWeather() {
        WeatherData data = generateRandomWeather();
        try {
            String json = objectMapper.writeValueAsString(data);
            kafkaTemplate.send(topic, data.getCity(), json);
            System.out.println("[Producer] Sent: " + json);
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    @Scheduled(fixedRate = 2000)
    public void scheduledSend() {
        sendRandomWeather();
    }

    public static class WeatherData {
        private final String city;
        private final String condition;
        private final int temperature;
        private final LocalDate date;

        @JsonCreator
        public WeatherData(@JsonProperty("city") String city,
                           @JsonProperty("condition") String condition,
                           @JsonProperty("temperature") int temperature,
                           @JsonProperty("date") LocalDate date) {
            this.city = city;
            this.condition = condition;
            this.temperature = temperature;
            this.date = date;
        }

        public String getCity() { return city; }
        public String getCondition() { return condition; }
        public int getTemperature() { return temperature; }
        public LocalDate getDate() { return date; }

        @Override
        public String toString() {
            return String.format("%s, %s, %d°C, %s", city, condition, temperature, date);
        }
    }
} 