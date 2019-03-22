package com.bigdata.hive.quanwen;

import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hive.conf.HiveConf;
import org.apache.hadoop.hive.metastore.HiveMetaStoreClient;
import org.apache.hadoop.hive.metastore.api.Database;
import org.apache.hadoop.hive.metastore.api.MetaException;
import org.apache.hadoop.hive.metastore.api.NoSuchObjectException;
import org.apache.hadoop.hive.metastore.api.Table;
import org.apache.hadoop.security.UserGroupInformation;
import org.apache.thrift.TException;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Properties;

public class DbHive {
	
	public DbHive(){
		
	}
	public List<String> getDataBases() throws MetaException, NoSuchObjectException, TException{
		Properties prop=new Properties();
		org.apache.hadoop.conf.Configuration conf = new org.apache.hadoop.conf.Configuration();
        conf.set("hadoop.security.authentication", "Kerberos");
        UserGroupInformation.setConfiguration(conf);
        try {
			UserGroupInformation.loginUserFromKeytab(prop.getProperty("principal"),prop.getProperty("keytab_path"));
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		HiveConf hiveConf = new HiveConf();
        hiveConf.addResource(new Path("/etc/hive/conf/hive-site.xml"));
        HiveMetaStoreClient hiveMetaStoreClient = new HiveMetaStoreClient(hiveConf);
        List<String> databaseList = hiveMetaStoreClient.getAllDatabases();
        List<String> dblist=new ArrayList();
out:    for(String dbName:databaseList){
            Database db = hiveMetaStoreClient.getDatabase(dbName);            
            String databaseName = db.getName();//全称
            
            List<String> tableList = hiveMetaStoreClient.getAllTables(databaseName);
            boolean flag = false;
            for(String tableName:tableList){
                Table table = hiveMetaStoreClient.getTable(databaseName,tableName);              
                Map<String,String> p = table.getParameters();
                if("lucene".equals(p.get("_preferformat"))) {
				    dblist.add(dbName);
				    continue out;
                }
            }
        }
        return dblist;
	}
}
