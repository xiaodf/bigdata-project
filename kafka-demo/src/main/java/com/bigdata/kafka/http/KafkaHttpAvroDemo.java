package com.bigdata.kafka.http;

import io.confluent.kafka.schemaregistry.client.CachedSchemaRegistryClient;
import org.apache.avro.Schema;
import org.apache.avro.generic.GenericData;
import org.apache.avro.generic.GenericDatumWriter;
import org.apache.avro.generic.GenericRecord;
import org.apache.avro.io.BinaryEncoder;
import org.apache.avro.io.EncoderFactory;
import org.apache.http.HttpEntity;
import org.apache.http.HttpResponse;
import org.apache.http.client.methods.HttpPost;
import org.apache.http.entity.ByteArrayEntity;
import org.apache.http.impl.client.DefaultHttpClient;

import java.io.ByteArrayOutputStream;
import java.io.UnsupportedEncodingException;
import java.nio.ByteBuffer;

import static org.apache.avro.Schema.Type.RECORD;

/**
 * Created by xdf on 9/19/2016.
 */
public class KafkaHttpAvroDemo {


	public static void main(String[] args) throws Exception {

		DefaultHttpClient httpClient = new DefaultHttpClient();

		// get http server list
		int curHTTPServer = 0;
		String[] ips = new String[]{"http://10.199.33.12:10080","http://10.199.33.13:10080","http://10.199.33.14:10080"};

		// get schema
		String topic = "test-ha";

		/**
		 * 从http://SchemaRegistryServer:8081/subjects/{topic}获取schema，该部分需要开发者自己实现
		 * 该URL的返回结果如下：
		 * {"subject":"test-ha","version":1,"id":81,"schema":"{\"type\":\"record\",\"name\":\"t_dams_ab_dname\",\"fields\":[{\"name\":\"C_TIME\",\"type\":\"string\"},{\"name\":\"C_PCODE\",\"type\":\"string\"},{\"name\":\"C_NODEIP\",\"type\":\"long\"},{\"name\":\"C_PROTOCOL\",\"type\":\"int\"},{\"name\":\"C_SIP\",\"type\":\"long\"},{\"name\":\"C_DIP\",\"type\":\"long\"},{\"name\":\"C_DOMAIN\",\"type\":\"string\"}]}"}
		 *
		 * */
		/**
		 * 该处为使用schema-registry客户端获取schema，并按照上面给出的格式说明解析出schema
		 */
		Schema.Parser parser = new Schema.Parser();
		CachedSchemaRegistryClient cachedSchemaRegistryClient = new CachedSchemaRegistryClient("http://10.199.33.14:8081,http://10.199.33.13:8081", 100);
		Schema schema = parser.parse(cachedSchemaRegistryClient.getLatestSchemaMetadata(topic).getSchema());

		System.out.println("*************** schema ***************");
		System.out.println(schema.toString());
		System.out.println("**************************************");

		//数据序列化
		GenericDatumWriter<GenericRecord> writer = new GenericDatumWriter<GenericRecord>(schema);
		// ~=10MB
		ByteArrayOutputStream out = new ByteArrayOutputStream(10000000);
		BinaryEncoder encoder = EncoderFactory.get().binaryEncoder(out, null);
		for (int i = 0; i < 10; i++) {
			// 构造一批数据，推荐大小为5MB左右
			out.reset();
			for (int j = 0; j < 1; j++) {
				GenericRecord record = new GenericData.Record(schema);
				//System.out.println("field size: " + schema.getFields().size());
				record.put("C_TIME","20170910");
				record.put("C_PCODE", "sushil");
				record.put("C_NODEIP", 20170808L);
				record.put("C_PROTOCOL", 17);
				record.put("C_SIP", 20170808L);
				record.put("C_DIP", 20170808L);
				record.put("C_DOMAIN", "201708098");

				writer.write(record, encoder);
			}
			// send to http server
			encoder.flush();
			out.flush();
			try {
				// load balance
				HttpPost request = new HttpPost(ips[curHTTPServer]);
				curHTTPServer++;
				curHTTPServer %= ips.length;

				// set header
				request.addHeader("content-type", "utf-8");
//            request.addHeader("User", "LiMing");
//            request.addHeader("Password", "123");
				request.addHeader("Topic", topic);
				request.addHeader("Format", "avro");
				HttpEntity httpEntity = new ByteArrayEntity(out.toByteArray());
				request.setEntity(httpEntity);
				//do not skip it!!!!!
				HttpResponse response = httpClient.execute(request);
				System.out.println("http return status: " + response.getStatusLine().getStatusCode());
			} catch (Exception ex) {
				// handle response here... try other servers
			} finally {
				httpClient.getConnectionManager().shutdown(); //Deprecated
			}
		}
		if (httpClient != null) {
			httpClient.getConnectionManager().shutdown(); //Deprecated
		}
	}

	public static Object convert(Schema schema, String v) throws Exception {
		Object o = null;
		if (v == null) {
			return null;
		}
		if (v.length() == 0 || v.equals("null")) {
			switch (schema.getType()) {
				case STRING:
					return v;
				case BYTES:
					return ByteBuffer.wrap(v.getBytes("UTF-8"));
				case UNION:
					break;
				default:
					return null;
			}
		}

		switch (schema.getType()) {
			case NULL:
				o = null;
				break;
			case BOOLEAN:
				o = Boolean.parseBoolean(v);
				break;
			case INT:
				o = Integer.parseInt(v);
				break;
			case LONG:
				o = Long.parseLong(v);
				break;
			case FLOAT:
				o = Float.parseFloat(v);
				break;
			case DOUBLE:
				o = Double.parseDouble(v);
				break;
			case BYTES:
				try {
					o = ByteBuffer.wrap(v.getBytes("UTF-8"));
				} catch (UnsupportedEncodingException e) {
					e.printStackTrace();
				}
				break;
			case STRING:
				o = v;
				break;
			case UNION:
				for (Schema mem : schema.getTypes()) {
					o = convert(mem, v);
					// break wehn encounter not null value, or we will get null
					if (o != null) break;
				}
				break;
			case RECORD:
				throw new Exception("Unsopported avro type:" + RECORD);
			case MAP:

				throw new Exception("Unsopported avro type:" + RECORD);
			case ENUM:
				throw new Exception("Unsopported avro type:" + RECORD);
			case ARRAY:
				throw new Exception("Unsopported avro type:" + RECORD);
			case FIXED:
				throw new Exception("Unsopported avro type:" + RECORD);
			default:
				throw new Exception("Unsopported avro type:" + RECORD);
		}
		return o;
	}
}


