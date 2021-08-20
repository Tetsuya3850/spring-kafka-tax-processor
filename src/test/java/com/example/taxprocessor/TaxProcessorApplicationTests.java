package com.example.taxprocessor;

import org.apache.kafka.clients.consumer.Consumer;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.LongDeserializer;
import org.apache.kafka.common.serialization.LongSerializer;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.apache.kafka.common.serialization.StringSerializer;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.kafka.annotation.EnableKafka;
import org.springframework.kafka.core.DefaultKafkaConsumerFactory;
import org.springframework.kafka.core.DefaultKafkaProducerFactory;
import org.springframework.kafka.support.serializer.JsonDeserializer;
import org.springframework.kafka.support.serializer.JsonSerializer;
import org.springframework.kafka.test.EmbeddedKafkaBroker;
import org.springframework.kafka.test.context.EmbeddedKafka;
import org.springframework.kafka.test.utils.KafkaTestUtils;

import java.util.Collections;
import java.util.Map;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.springframework.boot.test.context.SpringBootTest.WebEnvironment.RANDOM_PORT;

@EnableKafka
@EmbeddedKafka(
		partitions = 1,
		topics = { "originalPrice", "taxAddedPrice" },
		brokerProperties = { "listeners=PLAINTEXT://localhost:9092", "port=9092" }
)
@SpringBootTest
class TaxProcessorApplicationTests {

	@Autowired
	EmbeddedKafkaBroker embeddedKafkaBroker;

	private static final String ORIGINAL_PRICE_TOPIC = "originalPrice";
	private static final String TAX_ADDED_PRICE_TOPIC = "taxAddedPrice";

	@Test
	void contextLoads() {
		Map<String, Object> producerProps = KafkaTestUtils.producerProps(embeddedKafkaBroker);
		producerProps.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class);
		producerProps.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, LongSerializer.class);
		DefaultKafkaProducerFactory<String, Long> pf = new DefaultKafkaProducerFactory<>(producerProps);
		Producer<String, Long> producer = pf.createProducer();
		producer.send(new ProducerRecord<>(ORIGINAL_PRICE_TOPIC,  100L));

		Map<String, Object> consumerProps = KafkaTestUtils.consumerProps(
				"testT", "false", embeddedKafkaBroker
		);
		consumerProps.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class);
		consumerProps.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, LongDeserializer.class);
		DefaultKafkaConsumerFactory<String, Long> cf = new DefaultKafkaConsumerFactory<>(consumerProps);
		Consumer<String, Long> consumer = cf.createConsumer();
		consumer.subscribe(Collections.singleton(TAX_ADDED_PRICE_TOPIC));

		ConsumerRecord<String, Long> received = KafkaTestUtils.getSingleRecord(consumer, TAX_ADDED_PRICE_TOPIC);
		assertEquals(110L, received.value().longValue());
	}

}
