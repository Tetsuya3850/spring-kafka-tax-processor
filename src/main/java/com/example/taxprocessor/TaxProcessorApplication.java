package com.example.taxprocessor;

import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.messaging.handler.annotation.SendTo;

@SpringBootApplication
public class TaxProcessorApplication {

	public static void main(String[] args) {
		SpringApplication.run(TaxProcessorApplication.class, args);
	}

	@KafkaListener(id = "myId", topics = "originalPrice")
	@SendTo("taxAddedPrice")
	public long listen(long price) {
		return Math.round(price * 1.1);
	}

}
