package com.bigdata.kerberos;

import java.sql.*;

public class SecurityDao {
    private static String driverName = "com.mysql.jdbc.Driver";
    private Connection conn;
    private PreparedStatement ps;
    private Statement stmt;
    private ResultSet rs;
    public SecurityDao() {
        try {
            Class.forName(driverName);
        } catch (ClassNotFoundException e) {
            // TODO Auto-generated catch block
            e.printStackTrace();
            System.exit(1);
        }
    }

    public void insertUser(String host,String port,String dbUser,String dbPassWd,String user,String passWd) throws SQLException{
        String url = "jdbc:mysql://" + host + ":" + port+"/kerberos?characterEncoding=utf8";
        Connection conn = DriverManager.getConnection(url,dbUser,dbPassWd);
        String sql="insert into security values (?,?,?)";
        ps= conn.prepareStatement(sql);
        ps.setString(1,user);
        ps.setString(2,passWd);
        ps.setInt(3,1);
        ps.execute();
        ps.close();
    }
    
    @Deprecated
    public void insertUser(String host,String port,String user,String passWd) throws SQLException{
    	insertUser(host,port,"root","111111",user,passWd);
    }
    
    public void delUser(String host,String port,String dbUser,String dbPassWd,String user) throws SQLException{
        String url = "jdbc:mysql://" + host + ":" + port+"/kerberos?characterEncoding=utf8";
        Connection conn = DriverManager.getConnection(url,dbUser,dbPassWd);
        String sql="delete from security where user like \'"+user+"'";
        stmt=conn.createStatement();
        stmt.execute(sql);
    }
    
    @Deprecated
    public void delUser(String host,String port,String user) throws SQLException{
        delUser(host,port,"root","111111",user);
    }

    public void listUser(String host,String port,String dbUser,String dbPassWd) throws SQLException{
        String url = "jdbc:mysql://" + host + ":" + port+"/kerberos?characterEncoding=utf8";
        Connection conn = DriverManager.getConnection(url,dbUser,dbPassWd);
        String sql="select * from security";
        stmt=conn.createStatement();
        rs=stmt.executeQuery(sql);
        while(rs.next()){
            System.out.println(rs.getString("user")+","+rs.getString("passwd")+","+rs.getString("status"));
        }
        rs.close();
    }
    
    @Deprecated
    public void listUser(String host,String port) throws SQLException{
        listUser(host,port,"root","111111");
    }

    public void updateUser(String host,String port,String dbUser,String dbPassWd,String user,String passwd) throws SQLException{
        String url = "jdbc:mysql://" + host + ":" + port+"/kerberos?characterEncoding=utf8";
        Connection conn = DriverManager.getConnection(url,dbUser,dbPassWd);
        String sql="update security set passwd='"+passwd+"' where user='"+user+"'";
        stmt=conn.createStatement();
        stmt.execute(sql);
    }
    
    @Deprecated
    public void updateUser(String host,String port,String user,String passwd) throws SQLException{
        updateUser(host,port,"root","111111",user,passwd);
    }
    
    public void updateStatus(String host,String port,String dbUser,String dbPassWd,String user,int status) throws SQLException{
    	String url = "jdbc:mysql://" + host + ":" + port+"/kerberos?characterEncoding=utf8";
    	Connection conn = DriverManager.getConnection(url,dbUser,dbPassWd);
    	String sql="update security set status='"+status+"' where user='"+user+"'";
    	stmt=conn.createStatement();
    	stmt.execute(sql);
    }
    
    @Deprecated
    public void updateStatus(String host,String port,String user,int status) throws SQLException{
    	updateStatus(host,port,"root","111111",user,status);
    }

    public boolean checkLogin(String host,String port,String dbUser,String dbPassWd,String user,String passwd) throws SQLException{
        String url = "jdbc:mysql://" + host + ":" + port+"/kerberos?characterEncoding=utf8";
        Connection conn = DriverManager.getConnection(url,dbUser,dbPassWd);
        String sql="select * from security where user='"+user+"'";
        stmt=conn.createStatement();
        rs=stmt.executeQuery(sql);
        while(rs.next()){
            System.out.println(rs.getString("passwd"));
            if(rs.getString("passwd").equals(passwd)){
                rs.close();
                return true;
            }else {
                rs.close();
                return false;
            }
        }
        rs.close();
        return false;
    }
    
    @Deprecated
    public boolean checkLogin(String host,String port,String user,String passwd) throws SQLException{
        return checkLogin(host,port,"root","111111", user, passwd);
    }

    public static void main(String[] args) {
        SecurityDao sd=new SecurityDao();
        try {
            boolean b=sd.checkLogin("10.208.133.160","3306","victor1","123456");
            System.out.println(b);
        } catch (SQLException e) {
            // TODO Auto-generated catch block
            e.printStackTrace();
        }
    }
}
