package com.bigdata.hive.hcatalog;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hive.conf.HiveConf;
import org.apache.hadoop.hive.metastore.HiveMetaStoreClient;
import org.apache.hadoop.hive.metastore.api.*;
import org.apache.hadoop.hive.ql.io.RCFileInputFormat;
import org.apache.hadoop.hive.ql.io.RCFileOutputFormat;
import org.apache.hadoop.hive.serde.serdeConstants;
import org.apache.hadoop.hive.serde2.typeinfo.PrimitiveTypeInfo;
import org.apache.hive.hcatalog.common.HCatException;
import org.apache.hive.hcatalog.common.HCatUtil;
import org.apache.hive.hcatalog.data.schema.HCatFieldSchema;
import org.apache.hive.hcatalog.data.schema.HCatSchema;
import org.apache.thrift.TException;

import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

//import org.apache.hadoop.hive.metastore.api.Database;
//import org.apache.hadoop.hive.serde2.typeinfo.PrimitiveTypeInfo;

/**
 * 利用hcatalog接口生成hive表结构
 * 
 * 执行命令：java -cp test.jar:/etc/hive/conf:/etc/hadoop/conf
 * iie.hadoop.hcatalog.HCatSchemaTest
 * 
 * @author xdf
 */
public class HCatSchemaTest {
	
	public static void createTable(String dbName, String tblName, HCatSchema schema) {
		HiveMetaStoreClient client = null;
		try {
			HiveConf hiveConf = HCatUtil.getHiveConf(new Configuration());
			client = HCatUtil.getHiveClient(hiveConf);
		} catch (MetaException | IOException e) {
			e.printStackTrace();
		}

		try {
			// client.createDatabase(new Database(dbName, "", null, null));
			if (client.tableExists(dbName, tblName)) {
				client.dropTable(dbName, tblName);
			}
		} catch (TException e) {
			e.printStackTrace();
		}

		List<FieldSchema> fields = HCatUtil.getFieldSchemaList(schema
				.getFields());
		System.out.println(fields);
		Table table = new Table();
		table.setDbName(dbName);
		table.setTableName(tblName);

		StorageDescriptor sd = new StorageDescriptor();
		sd.setCols(fields);
		table.setSd(sd);
		sd.setInputFormat(RCFileInputFormat.class.getName());
		sd.setOutputFormat(RCFileOutputFormat.class.getName());
		sd.setParameters(new HashMap<String, String>());
		sd.setSerdeInfo(new SerDeInfo());
		sd.getSerdeInfo().setName(table.getTableName());
		sd.getSerdeInfo().setParameters(new HashMap<String, String>());
		sd.getSerdeInfo().getParameters()
				.put(serdeConstants.SERIALIZATION_FORMAT, "1");
		sd.getSerdeInfo().setSerializationLib(
				org.apache.hadoop.hive.serde2.columnar.ColumnarSerDe.class
						.getName());
		Map<String, String> tableParams = new HashMap<String, String>();
		table.setParameters(tableParams);
		try {
			client.createTable(table);
			System.out.println("Create table successfully!");
		} catch (TException e) {
			e.printStackTrace();
			return;
		} finally {
			client.close();
		}
	}

	@SuppressWarnings("deprecation")
	public static HCatSchema getHCatSchema(String[] fieldNames,
			String[] fieldTypes) throws HCatException {
		List<HCatFieldSchema> fieldSchemas = new ArrayList<HCatFieldSchema>(
				fieldNames.length);
		//PrimitiveTypeInfo typeInfo = new PrimitiveTypeInfo();		
		for (int i = 0; i < fieldNames.length; ++i) {
			PrimitiveTypeInfo typeInfo = new PrimitiveTypeInfo();
			typeInfo.setTypeName(fieldTypes[i].toUpperCase());
			System.out.println(typeInfo);
			HCatFieldSchema.Type type = HCatFieldSchema.Type
					.valueOf(fieldTypes[i].toUpperCase());
			fieldSchemas
					.add(new HCatFieldSchema(fieldNames[i],type,""));
		}
		return new HCatSchema(fieldSchemas);
	}

	public static void main(String[] args) throws HCatException {

		String[] fieldNames = { "col1", "col2" };
		String[] fieldTypes = { "int", "int" };
		HCatSchemaTest.createTable("xdf", "student",
				HCatSchemaTest.getHCatSchema(fieldNames, fieldTypes));
		System.exit(0);
	}
}
