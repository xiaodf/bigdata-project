package com.bigdata.kafka.simple;

import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;

import java.util.Arrays;

public class KafkaConsumer {

	public static void main(String[] args) {
        String topic = "testpasca";
        String brokerList = "10.199.33.14:9092";
        String groupName="group7";
		org.apache.kafka.clients.consumer.KafkaConsumer<String, String> consumer = KafkaUtil.getConsumer(groupName,brokerList);
        consumer.subscribe(Arrays.asList(topic));
        while(true) {
            ConsumerRecords<String, String> records = consumer.poll(10);
            for(ConsumerRecord<String, String> record : records) {
                System.out.println("fetched from partition " + record.partition() + ", offset: " + record.offset() + ", message: " + record.value());
            }
        }
	}
}
