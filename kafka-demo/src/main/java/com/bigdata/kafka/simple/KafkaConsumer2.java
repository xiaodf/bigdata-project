package com.bigdata.kafka.simple;

import kafka.consumer.Consumer;
import kafka.consumer.ConsumerConfig;
import kafka.consumer.ConsumerIterator;
import kafka.consumer.KafkaStream;
import kafka.javaapi.consumer.ConsumerConnector;

import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Properties;

public class KafkaConsumer2 {

	public static void main(String[] args) {

        String topic = "test3";

        Properties prop = new Properties();
        prop.put("zookeeper.connect", "10.199.33.14:2181/kafka");
        //prop.put("metadata.broker.list", "10.199.33.14:9092");
        prop.put("group.id", "group4");
        prop.put("auto.offset.reset", "smallest");
        prop.put("key.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");
        prop.put("value.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");
        ConsumerConnector consumer = Consumer.createJavaConsumerConnector(new ConsumerConfig(prop));
        Map<String, Integer> topicCountMap = new HashMap<String, Integer>();
        topicCountMap.put(topic, 1);
        Map<String, List<KafkaStream<byte[], byte[]>>> messageStreams = consumer.createMessageStreams(topicCountMap);
        final KafkaStream<byte[], byte[]> kafkaStream = messageStreams.get(topic).get(0);
        ConsumerIterator<byte[], byte[]> iterator = kafkaStream.iterator();
        while (iterator.hasNext()) {
            String msg = new String(iterator.next().message());
            System.out.println("--------"+msg);
        }
	}
}
