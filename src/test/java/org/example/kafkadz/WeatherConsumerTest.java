package org.example.kafkadz;

import com.fasterxml.jackson.databind.ObjectMapper;
import org.junit.jupiter.api.Test;
import java.time.LocalDate;
import static org.junit.jupiter.api.Assertions.*;
import java.util.List;
import com.fasterxml.jackson.datatype.jsr310.JavaTimeModule;

public class WeatherConsumerTest {
    private final ObjectMapper objectMapper = new ObjectMapper().registerModule(new JavaTimeModule());

    @Test
    void testParseWeatherData() throws Exception {
        WeatherProducer.WeatherData data = new WeatherProducer.WeatherData("Тюмень", "дождь", 10, LocalDate.now());
        String json = objectMapper.writeValueAsString(data);
        WeatherProducer.WeatherData parsed = objectMapper.readValue(json, WeatherProducer.WeatherData.class);
        assertEquals(data.getCity(), parsed.getCity());
        assertEquals(data.getCondition(), parsed.getCondition());
        assertEquals(data.getTemperature(), parsed.getTemperature());
        assertEquals(data.getDate(), parsed.getDate());
    }

    static class TestableWeatherConsumer extends WeatherConsumer {
        // Теперь можно напрямую обращаться к полям и методам
        public int getRainyDays(String city) {
            return rainyDaysByCity.getOrDefault(city, java.util.Collections.emptySet()).size();
        }
        public int getMaxTemperature() {
            return maxTemperature;
        }
        public String getHottestCity() {
            return hottestCity;
        }
        public java.time.LocalDate getHottestDate() {
            return hottestDate;
        }
        public double getAvgTemp(String city) {
            List<Integer> temps = tempsByCity.get(city);
            if (temps == null || temps.isEmpty()) return Double.NaN;
            return temps.stream().mapToInt(i -> i).average().orElse(Double.NaN);
        }
    }

    @Test
    void testAnalytics() {
        TestableWeatherConsumer consumer = new TestableWeatherConsumer();
        consumer.updateAnalytics(new WeatherProducer.WeatherData("Питер", "дождь", 10, LocalDate.of(2024, 7, 10)));
        consumer.updateAnalytics(new WeatherProducer.WeatherData("Питер", "солнечно", 25, LocalDate.of(2024, 7, 11)));
        consumer.updateAnalytics(new WeatherProducer.WeatherData("Питер", "дождь", 15, LocalDate.of(2024, 7, 12)));
        consumer.updateAnalytics(new WeatherProducer.WeatherData("Тюмень", "дождь", 8, LocalDate.of(2024, 7, 10)));
        consumer.updateAnalytics(new WeatherProducer.WeatherData("Тюмень", "дождь", 12, LocalDate.of(2024, 7, 11)));
        consumer.updateAnalytics(new WeatherProducer.WeatherData("Тюмень", "солнечно", 20, LocalDate.of(2024, 7, 12)));
        // Проверяем дождливые дни
        assertEquals(2, consumer.getRainyDays("Питер"));
        assertEquals(2, consumer.getRainyDays("Тюмень"));
        // Проверяем самую жаркую дату
        assertEquals(25, consumer.getMaxTemperature());
        assertEquals("Питер", consumer.getHottestCity());
        assertEquals(LocalDate.of(2024, 7, 11), consumer.getHottestDate());
        // Проверяем среднюю температуру
        assertEquals((10+25+15)/3.0, consumer.getAvgTemp("Питер"), 0.01);
        assertEquals((8+12+20)/3.0, consumer.getAvgTemp("Тюмень"), 0.01);
    }
}