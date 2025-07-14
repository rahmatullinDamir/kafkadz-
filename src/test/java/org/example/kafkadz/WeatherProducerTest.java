package org.example.kafkadz;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.datatype.jsr310.JavaTimeModule;
import org.junit.jupiter.api.Test;
import static org.junit.jupiter.api.Assertions.*;

public class WeatherProducerTest {
    private final WeatherProducer producer = new WeatherProducer(null);
    private final ObjectMapper objectMapper = new ObjectMapper().registerModule(new JavaTimeModule());

    @Test
    void testGenerateRandomWeather() {
        WeatherProducer.WeatherData data = producer.generateRandomWeather();
        assertNotNull(data.getCity());
        assertNotNull(data.getCondition());
        assertTrue(data.getTemperature() >= 0 && data.getTemperature() <= 35);
        assertNotNull(data.getDate());
    }

    @Test
    void testWeatherDataSerialization() throws Exception {
        WeatherProducer.WeatherData data = new WeatherProducer.WeatherData("Питер", "дождь", 15, java.time.LocalDate.now());
        String json = objectMapper.writeValueAsString(data);
        WeatherProducer.WeatherData parsed = objectMapper.readValue(json, WeatherProducer.WeatherData.class);
        assertEquals(data.getCity(), parsed.getCity());
        assertEquals(data.getCondition(), parsed.getCondition());
        assertEquals(data.getTemperature(), parsed.getTemperature());
        assertEquals(data.getDate(), parsed.getDate());
    }
}