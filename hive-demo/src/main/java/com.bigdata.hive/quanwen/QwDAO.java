package com.bigdata.hive.quanwen;

import org.apache.hadoop.hive.metastore.api.MetaException;
import org.apache.hadoop.hive.metastore.api.NoSuchObjectException;
import org.apache.thrift.TException;

import java.io.IOException;
import java.sql.*;
import java.util.ArrayList;
import java.util.List;
import java.util.Properties;

public class QwDAO {
	private Connection conn;
	private Statement stmt;
	private ResultSet rs;
	private PreparedStatement ps;
	static Properties prop=new Properties();
	
	public QwDAO(){
		conn= MysqlConn.getConnection();
	}
	
	public void insert(List<String> list2,List<Tables> list3) throws SQLException{
		for (Tables tables:list3){
			if(list2.contains(tables.getTablename())){
				String sql="update fulltext_monitor set count='"+tables.getCount()+"',quota='"+tables.getQuota()+"'where tablename='"+list2.get(1)+"'";
				stmt=conn.createStatement();
				stmt.execute(sql);
				stmt.close();
			}else{
				String sql="insert into fulltext_monitor value (?,?,?)";
				ps=conn.prepareStatement(sql);
				ps.setString(1,tables.getTablename());
				ps.setLong(2,tables.getCount());
				ps.setLong(3,tables.getQuota());
				ps.execute();
				ps.close();
			}
		}
	}
	
	public List<String> scan() throws SQLException{
		String sql="select * from fulltext_monitor";
		stmt=conn.createStatement();
		rs=stmt.executeQuery(sql);
		List<String> list=new ArrayList<String>();
		while(rs.next()){
			list.add(rs.getString(1));
		}
		rs.close();
		stmt.close();
		return list;
	}
	
	public static void main(String[] args) throws MetaException, NoSuchObjectException, TException, SQLException, IOException {
		QwDAO qwDAO=new QwDAO();
		QwHive qwHive=new QwHive();
		DbHive dbHive=new DbHive();
		List<String> list1=dbHive.getDataBases();
		System.out.println("----------------获取全文库成功----------------");
		for(String database:list1){
			System.out.println(database);
		}
		List<String> list2=qwDAO.scan();
		System.out.println("----------------获取mysql表成功----------------");
			// TODO check lucenes
				for(String database:list1){
					// TODO host
					try {
						System.out.println("----------------查询:"+database+"----------------");
						List<Tables> list3 = qwHive.getTables(prop.getProperty("hostname"),prop.getProperty("root"),prop.getProperty("passwd"),database);
						qwDAO.insert(list2, list3);
					} catch (ClassNotFoundException e) {
						e.printStackTrace();
					} catch (SQLException e) {
						e.printStackTrace();
					} catch (IOException e) {
						e.printStackTrace();
					}
				}
	}
}
