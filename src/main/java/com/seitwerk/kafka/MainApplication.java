package com.seitwerk.kafka;

import org.apache.logging.log4j.Level;
import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;

@SpringBootApplication
public class MainApplication {
    public static void main(String[] args) {
        String profile = System.getProperty("SPRING_PROFILES_ACTIVE", null);
        if(profile == null) {
            System.setProperty("spring.profiles.active", "dev");
        }

        SpringApplication.run(MainApplication.class, args);
    }
}
