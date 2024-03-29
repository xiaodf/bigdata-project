package com.bigdata.kafka.simple;
import org.apache.kafka.clients.producer.Callback;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.clients.producer.RecordMetadata;


public class KafkaProducer {
    public static void main(String[] args) throws Exception{
        String topic = args[0];
        String brokerList = args[1];

        Producer<String, String> producer = KafkaUtil.getProducer(brokerList);
        int i = 0;
        while(true) {
            ProducerRecord<String, String> record = new ProducerRecord<String, String>(topic, String.valueOf(i), "this is message"+i);
            producer.send(record, new Callback() {
                public void onCompletion(RecordMetadata metadata, Exception e) {
                    if (e != null)
                        e.printStackTrace();
                    System.out.println("message send to partition " + metadata.partition() + ", offset: " + metadata.offset());
                }
            });
            i++;
            Thread.sleep(100);
        }
    }
}