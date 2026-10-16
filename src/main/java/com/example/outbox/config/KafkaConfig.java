package com.example.outbox.config;

import org.apache.kafka.clients.admin.NewTopic;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.kafka.config.TopicBuilder;

@Configuration
public class KafkaConfig {

    @Bean
    NewTopic orderCreatedTopic() {
        return TopicBuilder.name(KafkaConstants.ORDER_CREATED_TOPIC).partitions(3).replicas(1).build();
    }
}
