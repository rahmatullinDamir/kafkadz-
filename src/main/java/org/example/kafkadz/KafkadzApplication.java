package org.example.kafkadz;

import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.scheduling.annotation.EnableScheduling;

@SpringBootApplication
@EnableScheduling
public class KafkadzApplication {

    public static void main(String[] args) {
        SpringApplication.run(KafkadzApplication.class, args);
    }

}
