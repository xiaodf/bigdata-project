package com.bigdata.kafka.avro;

import io.confluent.kafka.schemaregistry.client.CachedSchemaRegistryClient;
import io.confluent.kafka.schemaregistry.client.rest.exceptions.RestClientException;
import org.apache.avro.Schema;
import org.apache.avro.generic.GenericData;
import org.apache.avro.generic.GenericDatumReader;
import org.apache.avro.generic.GenericRecord;
import org.apache.avro.io.BinaryDecoder;
import org.apache.avro.io.DatumReader;
import org.apache.avro.io.DecoderFactory;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;

import java.io.IOException;
import java.util.Arrays;
import java.util.Properties;

public class KafkaAvroConsumer {
	public static void main(String[] args) {
        String topic=args[0];
        String group=args[1];

        Properties props =new Properties();
        props.put("max.partition.fetch.bytes", 10485760);
        props.put("auto.offset.reset", "earliest");
		props.put("bootstrap.servers", "10.199.33.14:9092");
        props.put("group.id",group);
        props.put("enable.auto.commit", "true");
        props.put("session.timeout.ms", "30000");
        props.put("key.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");
        props.put("value.deserializer", "org.apache.kafka.common.serialization.ByteArrayDeserializer");
        KafkaConsumer<String, byte[]> consumer = new KafkaConsumer<String, byte[]>(props);
        //获取schema
		try {
			Schema.Parser parser = new Schema.Parser();
			CachedSchemaRegistryClient cachedSchemaRegistryClient = new CachedSchemaRegistryClient("http://10.199.33.14:8081", 100);
			Schema schema = parser.parse(cachedSchemaRegistryClient.getLatestSchemaMetadata(topic).getSchema());
			
			DatumReader<GenericRecord> reader = new GenericDatumReader<GenericRecord>(schema);
			//获取topic
			consumer.subscribe(Arrays.asList(topic));
			GenericRecord grecord = new GenericData.Record(schema);
			
			//开始消费
			while(true) {
				ConsumerRecords<String, byte[]> records = consumer.poll(10);
				for(ConsumerRecord<String, byte[]> record : records) {
					//对每个record进行反序列化
					byte[] bytes=record.value();
					BinaryDecoder binaryDecoder = DecoderFactory.get().binaryDecoder(bytes, null);

					//对数据的业务操作实现在这里
					while (!binaryDecoder.isEnd()) {
						reader.read(grecord, binaryDecoder);
						System.out.println(grecord);
					}
				}
			}


		} catch (IOException e) {
			e.printStackTrace();
		} catch (RestClientException e) {
			e.printStackTrace();
		}
 }
}
