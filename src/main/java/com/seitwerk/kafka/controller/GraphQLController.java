package com.seitwerk.kafka.controller;

import com.seitwerk.kafka.config.KafkaConfiguration;
import com.seitwerk.kafka.entity.User;
import lombok.AllArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.apache.avro.Schema;
import org.apache.avro.generic.GenericData;
import org.apache.avro.generic.GenericRecord;
import org.apache.avro.reflect.ReflectData;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.springframework.graphql.data.GraphQlRepository;
import org.springframework.graphql.data.method.annotation.Argument;
import org.springframework.graphql.data.method.annotation.MutationMapping;
import org.springframework.web.bind.annotation.RestController;

@GraphQlRepository
@RestController
@AllArgsConstructor
@Slf4j
public class GraphQLController {

    Producer<Object, GenericRecord> kafkaProducer;

    private final static String TOPIC_NAME = "topic_58";

    @MutationMapping("registerUser")
    public GenericRecord registerUser(
            @Argument(name = "firstName") String firstName,
            @Argument(name = "lastName") String lastName,
            @Argument(name = "email") String email)
    {

        Schema schema = ReflectData.get().getSchema(User.class);
        GenericRecord userRecord = new GenericData.Record(schema);
        userRecord.put("firstName", firstName);
        userRecord.put("lastName", lastName);
        userRecord.put("email", email);
        userRecord.put("id", 1);

        ProducerRecord<Object, GenericRecord> record = new ProducerRecord<>(TOPIC_NAME, "1", userRecord);
        if(kafkaProducer != null) {
            kafkaProducer.send(record);
            kafkaProducer.flush();
        }

        return userRecord;
    }
}
