package org.example.kafkadz;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.datatype.jsr310.JavaTimeModule;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.stereotype.Component;

import java.time.LocalDate;
import java.util.*;

@Component
public class WeatherConsumer {
    private final ObjectMapper objectMapper = new ObjectMapper().registerModule(new JavaTimeModule());

    final Map<String, Set<LocalDate>> rainyDaysByCity = new HashMap<>();
    String hottestCity = null;
    LocalDate hottestDate = null;
    int maxTemperature = Integer.MIN_VALUE;
    final Map<String, List<Integer>> tempsByCity = new HashMap<>();

    @KafkaListener(topics = "${weather.topic}", groupId = "weather-group")
    public void listen(ConsumerRecord<String, String> record) {
        String json = record.value();
        try {
            WeatherProducer.WeatherData data = objectMapper.readValue(json, WeatherProducer.WeatherData.class);
            System.out.println("[Consumer] Received: " + data);
            updateAnalytics(data);
            printAnalytics();
        } catch (Exception e) {
            System.err.println("[Consumer] Failed to parse: " + json);
            e.printStackTrace();
        }
    }

    void updateAnalytics(WeatherProducer.WeatherData data) {
        if ("дождь".equals(data.getCondition())) {
            rainyDaysByCity.computeIfAbsent(data.getCity(), k -> new HashSet<>()).add(data.getDate());
        }
        if (data.getTemperature() > maxTemperature) {
            maxTemperature = data.getTemperature();
            hottestCity = data.getCity();
            hottestDate = data.getDate();
        }
        tempsByCity.computeIfAbsent(data.getCity(), k -> new ArrayList<>()).add(data.getTemperature());
    }

    void printAnalytics() {
        System.out.println("\n--- Аналитика ---");
        for (var entry : rainyDaysByCity.entrySet()) {
            System.out.printf("%s: дождливых дней за неделю: %d\n", entry.getKey(), entry.getValue().size());
        }
        if (hottestCity != null && hottestDate != null) {
            System.out.printf("Самая жаркая погода: %d°C, %s, %s\n", maxTemperature, hottestCity, hottestDate);
        }

        String coldestCity = null;
        double minAvg = Double.MAX_VALUE;
        for (var entry : tempsByCity.entrySet()) {
            double avg = entry.getValue().stream().mapToInt(i -> i).average().orElse(Double.NaN);
            if (avg < minAvg) {
                minAvg = avg;
                coldestCity = entry.getKey();
            }
        }
        if (coldestCity != null) {
            System.out.printf("Минимальная средняя температура: %.2f°C, %s\n", minAvg, coldestCity);
        }
        System.out.println("---\n");
    }
} 