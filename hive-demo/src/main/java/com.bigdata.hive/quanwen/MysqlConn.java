package com.bigdata.hive.quanwen;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;
import java.util.Properties;

public class MysqlConn {
	static Properties prop=new Properties();
	private static Connection conn;
	private static String driver="com.mysql.jdbc.Driver";
	private static String url="jdbc:mysql://"+prop.getProperty("dbhost")+"/"+prop.getProperty("dbname")+"?characterEncoding=utf8";
	
	private MysqlConn(){
			
	}
	
	static {
		try {
			Class.forName(driver);
			conn=DriverManager.getConnection(url,prop.getProperty("dbuser"),prop.getProperty("dbpasswd"));
		} catch (ClassNotFoundException e) {
			e.printStackTrace();
		} catch (SQLException e) {
			e.printStackTrace();
		}
	}
	
	public static Connection getConnection(){
		return conn;
	}
}
