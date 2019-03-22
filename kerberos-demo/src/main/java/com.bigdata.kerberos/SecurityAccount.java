package com.bigdata.kerberos;

import ch.ethz.ssh2.Connection;
import ch.ethz.ssh2.Session;
import ch.ethz.ssh2.StreamGobbler;
import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.List;

public class SecurityAccount {
	private SecurityDao securitydao;
	private SecurityPrinc securityprinc;
	
	public SecurityAccount(){
		securitydao=new SecurityDao();
		securityprinc=new SecurityPrinc();
		
	}
	//增加用户
	public boolean addUser(String host,String port,String username,String password,String user,String passwd) throws IOException{
		Connection conn= new Connection(host);
		Session ssh = null;
		//连接到主机
		conn.connect();
		//使用用户名和密码校验
		boolean isconn=conn.authenticateWithPassword(username, password);
		if(!isconn){  
            System.out.println("用户名称或者是密码不正确");  
        }else{
        	System.out.println("已经连接OK");
        	ssh=conn.openSession();
        	ssh.execCommand("clush -g all adduser "+user);
        	InputStream stdeer=new StreamGobbler(ssh.getStderr());
        	BufferedReader stdeerReader=new BufferedReader(new InputStreamReader(stdeer));
        	String line=stdeerReader.readLine();
//        	if(line==null||line.substring(line.length()-34).equals("the home directoryg already exists.")){
        	if(line==null){
        		ssh.close();
        		conn.close();
        		this.passWd(host, username, password, user, passwd);
        		try {
					securitydao.insertUser(host, port, user, passwd);
				} catch (SQLException e) {
					// TODO Auto-generated catch block
					e.printStackTrace();
				}
        		securityprinc.addPrinc(host, username, password, user,passwd);
        		return true;
        	}else{
        		if(line!=null){
        			System.out.println(line);
        		}
        		ssh.close();
        		conn.close();
        		return false;
        	}
        }
		ssh.close();
		conn.close();
		return false;
	}
	
	//给用户设置密码
	private boolean passWd(String host,String username,String password,String user,String passwd) throws IOException{
		Connection conn= new Connection(host);
		Session ssh = null;
		//连接到主机
		conn.connect();
		//使用用户名和密码校验
		boolean isconn=conn.authenticateWithPassword(username, password);
		if(!isconn){  
			System.out.println("用户名称或者是密码不正确");  
		}else{
			System.out.println("已经连接OK");
			ssh=conn.openSession();
			ssh.execCommand("clush -g all 'echo "+passwd+"|passwd --stdin "+user+"'");
			InputStream stdout=new StreamGobbler(ssh.getStdout());
			InputStream stdeer=new StreamGobbler(ssh.getStderr());
			BufferedReader stdoutReader=new BufferedReader(new InputStreamReader(stdout));
			BufferedReader stdeerReader=new BufferedReader(new InputStreamReader(stdeer));
			String line="";
			while((line=stdoutReader.readLine())!=null){
//				System.out.println(line);
				if(line.substring(line.length()-13).equals("successfully.")){
					ssh.close();
	        		conn.close();
					return true;
				}
			}
		}
		ssh.close();
		conn.close();
		return false;
	}
	
	//为用户更改密码
	public boolean updatePw(String host,String port,String username,String password,String user,String passwd) throws IOException{
		Connection conn= new Connection(host);
		Session ssh = null;
		//连接到主机
		conn.connect();
		//使用用户名和密码校验
		boolean isconn=conn.authenticateWithPassword(username, password);
		if(!isconn){  
			System.out.println("用户名称或者是密码不正确");  
		}else{
			System.out.println("已经连接OK");
			ssh=conn.openSession();
			ssh.execCommand("clush -g all 'echo "+passwd+"|passwd --stdin "+user+"'");
			InputStream stdout=new StreamGobbler(ssh.getStdout());
			InputStream stdeer=new StreamGobbler(ssh.getStderr());
			BufferedReader stdoutReader=new BufferedReader(new InputStreamReader(stdout));
			BufferedReader stdeerReader=new BufferedReader(new InputStreamReader(stdeer));
			String line="";
			while((line=stdoutReader.readLine())!=null){
//				System.out.println(line);
				if(line.substring(line.length()-13).equals("successfully.")){
					try {
						securitydao.updateUser(host, port, user, passwd);
					} catch (SQLException e) {
						// TODO Auto-generated catch block
						e.printStackTrace();
					}
					ssh.close();
					conn.close();
					return true;
				}
			}
		}
		ssh.close();
		conn.close();
		return false;
	}
	
	//增加工作组
	public boolean groupAdd(String host,String username,String password,String group) throws IOException{
		Connection conn= new Connection(host);
		Session ssh = null;
		//连接到主机
		conn.connect();
		//使用用户名和密码校验
		boolean isconn=conn.authenticateWithPassword(username, password);
		if(!isconn){  
			System.out.println("用户名称或者是密码不正确");  
		}else{
			System.out.println("已经连接OK");
			ssh=conn.openSession();
			ssh.execCommand("clush -g all groupadd "+group);
//			InputStream stdout=new StreamGobbler(ssh.getStdout());
			InputStream stdeer=new StreamGobbler(ssh.getStderr());
//			BufferedReader stdoutReader=new BufferedReader(new InputStreamReader(stdout));
			BufferedReader stdeerReader=new BufferedReader(new InputStreamReader(stdeer));
			String line=stdeerReader.readLine();
			if(line==null){
				ssh.close();
				conn.close();
				return true;
			}else{
				while((line=stdeerReader.readLine())!=null){
					System.out.println(line);
				}
				ssh.close();
				conn.close();
				return false;
			}
		}
		ssh.close();
		conn.close();
		return false;
	}
	
	//给已有用户增加工作组
	public boolean userMod(String host,String username,String password,String group,String user) throws IOException{
		Connection conn= new Connection(host);
		Session ssh = null;
		//连接到主机
		conn.connect();
		//使用用户名和密码校验
		boolean isconn=conn.authenticateWithPassword(username, password);
		if(!isconn){  
			System.out.println("用户名称或者是密码不正确");  
		}else{
			System.out.println("已经连接OK");
			ssh=conn.openSession();
			ssh.execCommand("clush -g all usermod -G "+group+" "+user);
			InputStream stdeer=new StreamGobbler(ssh.getStderr());
			BufferedReader stdeerReader=new BufferedReader(new InputStreamReader(stdeer));
			String line=stdeerReader.readLine();
			if(line==null){
				ssh.close();
				conn.close();
				return true;
			}else{
				while((line=stdeerReader.readLine())!=null){
					System.out.println(line);
				}
				ssh.close();
				conn.close();
				return false;
			}
		}
		ssh.close();
		conn.close();
		return false;
	}
	
	//从组中删除用户
	public boolean gpassWd(String host,String username,String password,String group,String user) throws IOException{
		Connection conn= new Connection(host);
		Session ssh = null;
		//连接到主机
		conn.connect();
		//使用用户名和密码校验
		boolean isconn=conn.authenticateWithPassword(username, password);
		if(!isconn){  
			System.out.println("用户名称或者是密码不正确");  
		}else{
			System.out.println("已经连接OK");
			ssh=conn.openSession();
			ssh.execCommand("clush -g all gpasswd -d "+user+" "+group);
			InputStream stdout=new StreamGobbler(ssh.getStdout());
			InputStream stdeer=new StreamGobbler(ssh.getStderr());
			BufferedReader stdoutReader=new BufferedReader(new InputStreamReader(stdout));
			BufferedReader stdeerReader=new BufferedReader(new InputStreamReader(stdeer));
			String line="";
			if((stdoutReader.readLine()!=null)&(stdeerReader.readLine()==null)){
				while((line=stdoutReader.readLine())!=null){
					System.out.println(line);
				}
				ssh.close();
				conn.close();
				return true;
			}else{
				while((line=stdeerReader.readLine())!=null){
					System.out.println(line);
				}
				ssh.close();
				conn.close();
				return false;
			}
		}
		ssh.close();
		conn.close();
		return false;
	}
	
	//永久删除用户
	public boolean userDel(String host,String port,String username,String password,String user) throws IOException{
		Connection conn= new Connection(host);
		Session ssh = null;
		//连接到主机
		conn.connect();
		//使用用户名和密码校验
		boolean isconn=conn.authenticateWithPassword(username, password);
		if(!isconn){  
			System.out.println("用户名称或者是密码不正确");  
		}else{
			System.out.println("已经连接OK");
			ssh=conn.openSession();
			ssh.execCommand("clush -g all userdel -r -f "+user);
			InputStream stdeer=new StreamGobbler(ssh.getStderr());
			BufferedReader stdeerReader=new BufferedReader(new InputStreamReader(stdeer));
			String line=stdeerReader.readLine();
			if(line==null||line.contains("userdel:")){
				ssh.close();
				conn.close();
				try {
					securitydao.delUser(host, port, user);
				} catch (SQLException e) {
					// TODO Auto-generated catch block
					e.printStackTrace();
				}
				securityprinc.delPrinc(host, username, password, user);
				return true;
			}else{
				while((line=stdeerReader.readLine())!=null){
					System.out.println(line);
				}
				ssh.close();
				conn.close();
				return false;
			}
		}
		ssh.close();
		conn.close();
		return false;
	}
	
	//删除用户组
	public boolean groupDel(String host,String username,String password,String group) throws IOException{
		Connection conn= new Connection(host);
		Session ssh = null;
		//连接到主机
		conn.connect();
		//使用用户名和密码校验
		boolean isconn=conn.authenticateWithPassword(username, password);
		if(!isconn){  
			System.out.println("用户名称或者是密码不正确");  
		}else{
			System.out.println("已经连接OK");
			ssh=conn.openSession();
			ssh.execCommand("clush -g all groupdel "+group);
			InputStream stdeer=new StreamGobbler(ssh.getStderr());
			BufferedReader stdeerReader=new BufferedReader(new InputStreamReader(stdeer));
			String line=stdeerReader.readLine();
			if(line==null){
				ssh.close();
				conn.close();
				return true;
			}else{
				while((line=stdeerReader.readLine())!=null){
					System.out.println(line);
				}
				ssh.close();
				conn.close();
				return false;
			}
		}
		ssh.close();
		conn.close();
		return false;
	}
	
	//返回用户列表
	public List<String> userList(String host,String username,String password) throws IOException{
		Connection conn= new Connection(host);
		Session ssh = null;
		//连接到主机
		conn.connect();
		//使用用户名和密码校验
		boolean isconn=conn.authenticateWithPassword(username, password);
		List<String> list=new ArrayList<String>();
		if(!isconn){  
			System.out.println("用户名称或者是密码不正确");  
		}else{
			System.out.println("已经连接OK");
			ssh=conn.openSession();
			ssh.execCommand("cat /etc/shadow");
			InputStream stdout=new StreamGobbler(ssh.getStdout());
			BufferedReader stdoutReader=new BufferedReader(new InputStreamReader(stdout));
			String line="";
			while((line=stdoutReader.readLine())!=null){
				System.out.println();
				list.add(line.substring(0,line.indexOf(":")));
			}
		}
		return list;
	}
	
	public static void main(String[] args) {
		/*SecurityAccount a=new SecurityAccount();
		try {
//			boolean b=a.groupAdd("172.16.18.201","root","111111","1");
//			boolean c=a.addUser("10.208.133.160","3306","root","iie@509!","victor","111");
			boolean c=a.userDel("10.208.133.160","3306","root","iie@509!","victor");
			System.out.println(c);
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}*/
	}
}
