package com.seitwerk.kafka.config;

import com.seitwerk.kafka.core.annotation.KafkaListener;
import io.confluent.kafka.schemaregistry.avro.AvroSchema;
import lombok.extern.slf4j.Slf4j;
import org.apache.avro.Schema;
import org.apache.avro.generic.GenericRecord;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.springframework.context.annotation.Configuration;

@Slf4j
@Configuration
public class KafkaListenerConfiguration {

    @KafkaListener(topic = "topic_58")
    public void onConsumeEvent(ConsumerRecord<Object, GenericRecord> record) {
        Schema schema = record.value().getSchema();
        AvroSchema avroSchema = new AvroSchema(schema);

        System.out.println("TYPE = " + schema.getFullName() + ", value = " + record.value());

        log.info("CONSUME EVENT RECEIVED: {}", record);
    }
}
