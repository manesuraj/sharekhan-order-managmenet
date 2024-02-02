package com.myproject.OrderManagment;

import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.kafka.annotation.EnableKafkaStreams;

@SpringBootApplication
@EnableKafkaStreams
public class OrderManagmentApplication {

	public static void main(String[] args) {
		SpringApplication.run(OrderManagmentApplication.class, args);
	}

}
