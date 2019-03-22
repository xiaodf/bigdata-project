package com.bigdata.jdbc;

import org.apache.hadoop.security.UserGroupInformation;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.Statement;

/*
 * 开启权限验证时，可以传入用户principal 和 keytab 进行身份验证
 */
public class sparkjdbc {
	public static void main(String args[]) {
		String sql = args[0];
		String principal = args[1];
		String keytab = args[2];
		try {		
			org.apache.hadoop.conf.Configuration conf = new org.apache.hadoop.conf.Configuration();
			conf.set("hadoop.security.authentication", "Kerberos");
			UserGroupInformation.setConfiguration(conf);
			UserGroupInformation.loginUserFromKeytab(principal,keytab);
			System.out.println("getting connection");
			System.out.println("current user: "+UserGroupInformation.getCurrentUser());
			System.out.println("login user: "+UserGroupInformation.getLoginUser());
			
			Class.forName("org.apache.hive.jdbc.HiveDriver");
			Connection con = DriverManager
					.getConnection("jdbc:hive2://t162:10001/;principal=hive/t162@HADOOP.COM");
			System.out.println("got connection");
			Statement stmt = con.createStatement();
			ResultSet rs = stmt.executeQuery(sql);// executeQuery会返回结果的集合，否则返回空值
            System.out.println("打印输出结果：");
            while (rs.next()) {
                System.out.println(rs.getString(1));// 入如果返回的是int类型可以用getInt()
            }
			
			con.close();
		} catch (Exception e) {
			e.printStackTrace();
		}
	}
}