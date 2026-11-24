package com.example.orderhub;

import org.apache.kafka.clients.admin.NewTopic;
import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.context.annotation.Bean;
import org.springframework.kafka.config.TopicBuilder;
import org.springframework.scheduling.annotation.EnableScheduling;

@SpringBootApplication
@EnableScheduling
public class OrderhubApplication {

    public static void main(String[] args) {
        SpringApplication.run(OrderhubApplication.class, args);
    }

    @Bean
    NewTopic ordersCreatedTopic() {
        return TopicBuilder.name("orders.created").partitions(3).replicas(1).build();
    }
}
