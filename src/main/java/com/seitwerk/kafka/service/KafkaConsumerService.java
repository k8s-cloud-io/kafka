package com.seitwerk.kafka.service;

import com.seitwerk.kafka.config.KafkaConfiguration;
import com.seitwerk.kafka.core.annotation.KafkaListener;
import io.confluent.kafka.serializers.AbstractKafkaSchemaSerDeConfig;
import io.confluent.kafka.serializers.KafkaAvroDeserializer;
import jakarta.annotation.PostConstruct;
import lombok.extern.slf4j.Slf4j;
import org.apache.avro.generic.GenericRecord;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.springframework.context.ApplicationContext;
import org.springframework.context.annotation.Configuration;
import org.springframework.scheduling.annotation.EnableScheduling;
import org.springframework.scheduling.annotation.Scheduled;

import java.lang.reflect.Method;
import java.lang.reflect.Parameter;
import java.time.Duration;
import java.util.ArrayList;
import java.util.List;
import java.util.Properties;

@Slf4j
@Configuration
@EnableScheduling
public class KafkaConsumerService {
    private final static Properties props = new Properties();
    private static KafkaConsumer<Object, GenericRecord> consumer;
    private static ApplicationContext applicationContext;
    private final static List<KafkaListenerHolder> listeners = new ArrayList<>();

    public KafkaConsumerService(KafkaConfiguration conf, ApplicationContext context) {
        props.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, conf.getBootServers());
        props.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class);
        props.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, KafkaAvroDeserializer.class);
        props.put(AbstractKafkaSchemaSerDeConfig.SCHEMA_REGISTRY_URL_CONFIG, conf.getSchemaRegistryUrl());
        props.put(ConsumerConfig.GROUP_ID_CONFIG, conf.getGroupId());
        props.put(ConsumerConfig.FETCH_MAX_WAIT_MS_CONFIG, 100);
        applicationContext = context;
    }

    @PostConstruct
    public void start() {
        consumer = new KafkaConsumer<>(props);
        List<String> topicList = new ArrayList<>();

        String[] beans = applicationContext.getBeanNamesForAnnotation(Configuration.class);
        List<String> types = List.of(applicationContext.getBeanNamesForType(KafkaConsumerService.class));
        for(String beanName: beans) {
            if(types.contains(beanName)) {
                continue;
            }

            Object bean = applicationContext.getBean(beanName);
            for(Method method: bean.getClass().getMethods()) {
                if(method.isAnnotationPresent(KafkaListener.class)) {
                    KafkaListener l = method.getAnnotation(KafkaListener.class);
                    KafkaListenerHolder holder = new KafkaListenerHolder();
                    holder.instance = bean;
                    holder.method = method;
                    holder.topic = l.topic();
                    listeners.add(holder);
                    topicList.add(holder.topic);
                }
            }
        }
        consumer.subscribe(topicList);
    }

    @Scheduled(fixedRate = 100)
    public void work() {
        ConsumerRecords<Object, GenericRecord> records =
                consumer.poll(Duration.ofMillis(100));

        for (ConsumerRecord<Object, GenericRecord> record : records) {
            for(KafkaListenerHolder holder: listeners) {
                if(holder.topic.compareTo(record.topic()) == 0) {
                    try {
                        Method method = holder.method;
                        if(method.getParameterCount() > 1) {
                            throw new RuntimeException("too many parameters");
                        }
                        if(method.getParameterCount() == 1) {
                            Parameter param = method.getParameters()[0];
                            String paramType = param.getType().getName();
                            if(paramType.compareTo(ConsumerRecord.class.getName()) == 0) {
                                Object[] parameters = new Object[1];
                                parameters[0] = record;
                                holder.method.invoke(holder.instance, parameters);
                            } else {
                                throw new RuntimeException("unsupported parameter type %s".formatted(paramType));
                            }
                        }
                        else {
                            holder.method.invoke(holder.instance);
                        }
                    }
                    catch(Exception e) {
                        log.error("unable to invoke listener {} with method {}: {}",
                                holder.instance.getClass().getName(),
                                holder.method.getName(),
                                e.getMessage()
                        );
                    }
                }
            }
        }
    }

    private static class KafkaListenerHolder {
        private Object instance;
        private Method method;
        private String topic;
    }
}
