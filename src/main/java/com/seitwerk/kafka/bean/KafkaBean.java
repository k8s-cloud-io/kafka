package com.seitwerk.kafka.bean;

import com.seitwerk.kafka.config.KafkaConfiguration;
import io.confluent.kafka.serializers.AbstractKafkaSchemaSerDeConfig;
import io.confluent.kafka.serializers.KafkaAvroSerializer;
import lombok.Getter;
import lombok.Setter;
import lombok.extern.slf4j.Slf4j;
import org.apache.avro.generic.GenericRecord;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.common.serialization.StringSerializer;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

import java.util.Properties;

@Slf4j
@Getter
@Setter
@Configuration
public class KafkaBean {
    private static final Properties producerProps = new Properties();

    public KafkaBean(KafkaConfiguration conf) {
        producerProps.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, conf.getBootServers());
        producerProps.put(AbstractKafkaSchemaSerDeConfig.SCHEMA_REGISTRY_URL_CONFIG, conf.getSchemaRegistryUrl());
        producerProps.put(ProducerConfig.ACKS_CONFIG, "all");
        producerProps.put(ProducerConfig.RETRIES_CONFIG, 0);
        producerProps.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class);
        producerProps.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, KafkaAvroSerializer.class);
        producerProps.put(ProducerConfig.TRANSACTION_TIMEOUT_CONFIG, 100);
    }

    @Bean
    public KafkaProducer<Object, GenericRecord> kafkaProducer() {
        return new KafkaProducer<>(producerProps);
    }
}
