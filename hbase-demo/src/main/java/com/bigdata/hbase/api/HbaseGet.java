package com.bigdata.hbase.api;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.Cell;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.*;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.security.UserGroupInformation;
import org.json.JSONException;
import org.json.JSONObject;

import java.io.IOException;

public class HbaseGet {

    private static Admin admin;

    public static void main(String[] args) {
        try {
            System.out.println("--------------------根据rowKey查询--------------------");
            HbaseGet test = new HbaseGet();
            JSONObject result = test.getDataByRowKey("infodata:newsinfo_anticheat_user_blacklist_data", "596d9573f2fcd90001f87f3");
            System.out.println(result);
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    //连接集群
    public static Connection initHbase() throws IOException {

        Configuration conf = HBaseConfiguration.create();

        //kerberos权限认证
        String principal = "infodata/10.31.103.113@HERACLES.SOHUNO.COM";
        String keytab= "D:/infodata.keytab_10.31.103.113";
        System. setProperty("java.security.krb5.conf", "D:/krb5.conf" );
        conf.set("hadoop.security.authentication", "Kerberos");
        UserGroupInformation.setConfiguration(conf);
        UserGroupInformation.loginUserFromKeytab(principal,keytab);
        System.out.println("getting connection");
        System.out.println("current user: "+UserGroupInformation.getCurrentUser());
        System.out.println("login user: "+UserGroupInformation.getLoginUser());

        //hbase集群配置
        conf.set("hbase.zookeeper.quorum", "dsrv2.heracles.sohuno.com,drm2.heracles.sohuno.com,dmeta2.heracles.sohuno.com,dmeta1.heracles.sohuno.com,drm1.heracles.sohuno.com");
        conf.set("hbase.zookeeper.property.clientPort", "2181");
        conf.set("zookeeper.znode.parent", "/hbase-secure");
        Connection connection = ConnectionFactory.createConnection(conf);
        return connection;
    }

    //根据rowKey进行查询
    public JSONObject getDataByRowKey(String tableName, String rowKey) throws IOException {
        Table table = initHbase().getTable(TableName.valueOf(tableName));
        // 根据主键查询
        Get get = new Get(rowKey.getBytes());
        JSONObject jsonObjectResult = new JSONObject();
        try {
            jsonObjectResult.put("userid",rowKey);
            if (!get.isCheckExistenceOnly()) {
                Result result = table.get(get);
                if(result.size() > 0){
                    for (Cell cell : result.rawCells()) {
                        String colName = Bytes.toString(cell.getQualifierArray(), cell.getQualifierOffset(), cell.getQualifierLength());
                        String value = Bytes.toString(cell.getValueArray(), cell.getValueOffset(), cell.getValueLength());
                        //构建result json字符串
                        jsonObjectResult.put(colName,value);
                    }
                }else {
                    //如果根据rowkey查询结果为空,返回null
                    return null;
                }
            }
        } catch (JSONException e) {
            e.printStackTrace();
        }

        return jsonObjectResult;
    }

}
