package com.bigdata.hive.quanwen;

import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hive.conf.HiveConf;
import org.apache.hadoop.hive.metastore.HiveMetaStoreClient;
import org.apache.hadoop.hive.metastore.api.MetaException;
import org.apache.hadoop.hive.metastore.api.NoSuchObjectException;
import org.apache.hadoop.hive.metastore.api.Table;
import org.apache.hadoop.security.UserGroupInformation;
import org.apache.thrift.TException;

import java.io.IOException;
import java.sql.*;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Properties;

public class QwHive {

	public QwHive(){
		
	}
	
	public List<Tables> getTables(String hostname, String username, String password, String database) throws ClassNotFoundException, SQLException, IOException, MetaException, NoSuchObjectException, TException {
		Properties prop=new Properties();
		org.apache.hadoop.conf.Configuration conf = new org.apache.hadoop.conf.Configuration();
        conf.set("hadoop.security.authentication", "Kerberos");
        UserGroupInformation.setConfiguration(conf);
        UserGroupInformation.loginUserFromKeytab(prop.getProperty("principal"),prop.getProperty("keytab_path"));
        HiveConf hiveConf = new HiveConf();
        hiveConf.addResource(new Path("/etc/hive/conf/hive-site.xml"));
        HiveMetaStoreClient hiveMetaStoreClient = new HiveMetaStoreClient(hiveConf);
        
        Class.forName("org.apache.hive.jdbc.HiveDriver");
        // TODO node1a1
        Connection conn = DriverManager.getConnection(prop.getProperty("url"));
        Statement stmt = conn.createStatement();
        String sql="show tables in "+database;
        ResultSet rs = stmt.executeQuery(sql);
        List<Tables> list=new ArrayList<Tables>();
        QwCount qwCount=new QwCount();
        while (rs.next()) {
        	 System.out.println(rs.getString(1));
        	 Table table = hiveMetaStoreClient.getTable(database,rs.getString(1));              
             Map<String,String> p = table.getParameters();
             if("lucene".equals(p.get("_preferformat"))) {
            	 System.out.println("开始查询全文表：----------------"+rs.getString(1));
            	 Tables tables=new Tables();
            	 tables.setTablename(database+"."+rs.getString(1));
            	 tables.setCount(this.getCount(conn,database,rs.getString(1)));
            	 tables.setQuota(qwCount.chaxun(hostname, username, password, database,rs.getString(1)));
            	 list.add(tables);
             }
        }
        rs.close();
        stmt.close();
        conn.close();
		return list;
	}
	
	private long getCount(Connection conn,String database,String table) {
		try {
			String sql="select count(*) from "+database+"."+table;
			Statement stmt = conn.createStatement();
			ResultSet rs=stmt.executeQuery(sql);
			if(rs.next()){
				return rs.getLong(1);
			}
			rs.close();
			stmt.close();
		} catch (SQLException e) {
			// TODO Auto-generated catch block
			return -1;
		}
		return 0;
	}
}
