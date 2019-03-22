package com.bigdata.kafka.simple;

import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.clients.producer.KafkaProducer;

import java.io.IOException;
import java.util.Properties;

public class KafkaUtil {
	private static KafkaProducer<String, String> kp;
	private static KafkaConsumer<String, String> kc;

    public static KafkaProducer<String, String> getProducer(String brokerList) throws IOException {
        if (kp == null) {
            Properties props = new Properties();
//			props.put("sasl.kerberos.service.name", "kafka");
//			props.put("sasl.mechanism", "GSSAPI");
//			props.put("security.protocol", "SASL_PLAINTEXT");
            props.put("bootstrap.servers", brokerList);
            props.put("acks", "1");
            props.put("retries", 0);
            props.put("batch.size", 16384);
            props.put("key.serializer", "org.apache.kafka.common.serialization.StringSerializer");
            props.put("value.serializer", "org.apache.kafka.common.serialization.StringSerializer");
            kp = new KafkaProducer<String, String>(props);
        }
        return kp;
    }
    public static KafkaConsumer<String, String> getConsumer(String groupName, String brokerList) {
        if(kc == null) {
            Properties props = new Properties();
//			props.put("sasl.kerberos.service.name", "kafka");
//			props.put("sasl.mechanism", "GSSAPI");
//			props.put("security.protocol", "SASL_PLAINTEXT");
            props.put("bootstrap.servers", brokerList);
            props.put("group.id", groupName);
           // props.put("enable.auto.commit", "true");
            //props.put("auto.commit.interval.ms", "1000");
            //props.put("session.timeout.ms", "30000");
            props.put("auto.offset.reset", "earliest");
            props.put("key.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");
            props.put("value.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");
            kc = new KafkaConsumer<String, String>(props);
        }
        return kc;
    }
}
