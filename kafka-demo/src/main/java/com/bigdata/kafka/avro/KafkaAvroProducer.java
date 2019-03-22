package com.bigdata.kafka.avro;

import io.confluent.kafka.schemaregistry.client.CachedSchemaRegistryClient;
import org.apache.avro.Schema;
import org.apache.avro.generic.GenericData;
import org.apache.avro.generic.GenericDatumWriter;
import org.apache.avro.generic.GenericRecord;
import org.apache.avro.io.BinaryEncoder;
import org.apache.avro.io.EncoderFactory;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.ProducerRecord;

import java.io.ByteArrayOutputStream;
import java.util.Properties;


public class KafkaAvroProducer {
	 public static void main(String[] args) throws Exception{
		    //获取topic
		    String topic="test-av";

		    Properties props =new Properties();
	   	    props.put("bootstrap.servers", "10.199.33.14:9092");
	   	    props.put("acks", "1");
	   	    props.put("retries", 0);
	   	    props.put("batch.size", 16384);
	   	    props.put("key.serializer", "org.apache.kafka.common.serialization.StringSerializer");
	   	    props.put("value.serializer", "org.apache.kafka.common.serialization.ByteArraySerializer");
            Producer<String, byte[]> producer = new KafkaProducer<String, byte[]>(props);

	        //获取schema
	    	Schema.Parser parser = new Schema.Parser();
			CachedSchemaRegistryClient cachedSchemaRegistryClient = new CachedSchemaRegistryClient("http://10.199.33.14:8081", 100);
			Schema schema = parser.parse(cachedSchemaRegistryClient.getLatestSchemaMetadata(topic).getSchema());
			
			ByteArrayOutputStream out = new ByteArrayOutputStream(10240);
			BinaryEncoder encoder = EncoderFactory.get().binaryEncoder(out, null);
			GenericDatumWriter<GenericRecord> writer = new GenericDatumWriter(schema);
			
			for (int i = 0; i < 1000000; i+=2) {
				GenericRecord record = new GenericData.Record(schema);
				record.put("C_TIME","20170910");
				record.put("C_PCODE", "sushil");
				record.put("C_NODEIP", 20170808L);
				record.put("C_PROTOCOL", 17);
				record.put("C_SIP", 20170808L);
				record.put("C_DIP", 20170808L);
				record.put("C_DOMAIN", "201708098");
				writer.write(record, encoder);
				if (i % 100 == 0) {
					encoder.flush();
					out.flush();
					byte[] payload = out.toByteArray();
					producer.send(new ProducerRecord<String, byte[]>(topic, payload));
					System.out.println("message send to partition " + record);
					out.reset();
				}
			}
			encoder.flush();
			out.flush();
			if (out.size() > 0) {
				byte[] payload = out.toByteArray();
				producer.send(new ProducerRecord<String, byte[]>(topic, payload));
			}
	 }		
}
