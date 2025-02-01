package com.seitwerk.kafka.config;

import lombok.Getter;
import lombok.Setter;
import org.springframework.boot.context.properties.ConfigurationProperties;
import org.springframework.stereotype.Component;

@Getter
@Setter
@Component
@ConfigurationProperties(prefix = "spring.kafka")
public class KafkaConfiguration {
    private String bootServers;
    private String schemaRegistryUrl;
    private String groupId;
}
