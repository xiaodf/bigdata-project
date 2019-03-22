package com.bigdata.hbase.api;

import net.sf.json.JSONObject;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hbase.Cell;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.*;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.security.UserGroupInformation;

import java.io.IOException;

/**
 * Created by xdf on 2019/3/4.
 */
public class HBaseUtil {

    //根据rowKey进行查询
    public JSONObject getDataByRowKey(String tableName, String rowKey, String rowkeyColum) throws IOException {
        Table table = this.initHbase().getTable(TableName.valueOf(tableName));
        // 根据主键查询
        Get get = new Get(rowKey.getBytes());
        JSONObject jsonObjectResult = new JSONObject();
        jsonObjectResult.put(rowkeyColum,rowKey);
        if (!get.isCheckExistenceOnly()) {
            Result result = table.get(get);
            if(result.size() > 0){
                for (Cell cell : result.rawCells()) {
                    String colName = Bytes.toString(cell.getQualifierArray(), cell.getQualifierOffset(), cell.getQualifierLength());
                    String value = Bytes.toString(cell.getValueArray(), cell.getValueOffset(), cell.getValueLength());
                    //构建result json字符串
                    jsonObjectResult.put(colName,value);
                }
                //根据rowkey查询hbase命中
                jsonObjectResult.put("isHit","true");
            }else {
                //根据rowkey查询结果为空,返回未命中
                jsonObjectResult.put("isHit","false");
            }
        }
        return jsonObjectResult;
    }

    //连接hbase集群
    public Connection initHbase() throws IOException {
        Configuration conf = HBaseConfiguration.create();

        //kerberos权限认证
        String path = PropertiesUtil.getProperty("spring.conf.common.path");
        String principal = PropertiesUtil.getProperty("hbase.datasource.kerberos.principal");
        String keytab = PropertiesUtil.getProperty("hbase.datasource.kerberos.keytab");

        System. setProperty("java.security.krb5.conf",path+"/krb5.conf");
        conf.addResource(new Path(path+"/hbase-site.xml"));
        conf.set("hadoop.security.authentication", "Kerberos");
        conf.set("fs.hdfs.impl", "org.apache.hadoop.hdfs.DistributedFileSystem");
        UserGroupInformation.setConfiguration(conf);
        UserGroupInformation.loginUserFromKeytab(principal,path+"/"+keytab);
        System.out.println("login user: "+UserGroupInformation.getLoginUser());

        //hbase集群配置
        conf.set("hbase.zookeeper.quorum", PropertiesUtil.getProperty("hbase.datasource.hbase.zookeeper.quorum"));
        conf.set("hbase.zookeeper.property.clientPort", PropertiesUtil.getProperty("hbase.datasource.hbase.zookeeper.property.clientPort"));
        conf.set("zookeeper.znode.parent", PropertiesUtil.getProperty("hbase.datasource.zookeeper.znode.parent"));
        Connection connection = ConnectionFactory.createConnection(conf);
        return connection;
    }

}
