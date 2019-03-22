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

public class SecurityPrinc {
	SecurityDao securityDao=new SecurityDao();
	
	public SecurityPrinc(){
		
	}
        //生成principal
        public boolean addPrinc(String hostname,String username,String password,String princName,String passwd) throws IOException {
            Connection conn= new Connection(hostname);
            Session ssh = null;
            conn.connect();
            boolean isconn=conn.authenticateWithPassword(username, password);
            if(!isconn){  
                System.out.println("用户名称或者是密码不正确");  
            }else{
            	System.out.println("已经连接OK");
            	ssh=conn.openSession();
//	        	ssh.execCommand("sh /home/iie/kerberos-util/addprinc.sh "+princName);
            	ssh.execCommand("kadmin -p cloudera-scm/admin -w 111111 -q 'addprinc -pw "+passwd+" "+princName+"'");
            	InputStream is=new StreamGobbler(ssh.getStdout());
            	BufferedReader br=new BufferedReader(new InputStreamReader(is));
            	String line="";
            	while((line=br.readLine())!=null){
            		if(line.substring(line.length()-8).equals("created.")){
        				br.close();
        				ssh.close();
        				conn.close();
//	    				this.addKey(hostname, username, password, princName);
        				return true;
        			}
            	}
            }
            ssh.close();
            conn.close();
            return false;
        }
        
        //为principal设置锁定
        public boolean lockPrinc(String hostname,String port,String username,String password,String princName,String ipStr) throws IOException {
            Connection conn= new Connection(hostname);
            Session ssh = null;
            conn.connect();
            boolean isconn=conn.authenticateWithPassword(username, password);
            if(!isconn){  
                System.out.println("用户名称或者是密码不正确");  
            }else{
            	System.out.println("已经连接OK");
            	ssh=conn.openSession();
//	        	ssh.execCommand("sh /home/iie/kerberos-util/addprinc.sh "+princName);
            	String cmd = "kadmin -p cloudera-scm/admin -w 111111 -q 'modprinc +requires_preauth -allow_svr "+
            			princName+"';kadmin -p cloudera-scm/admin -w 111111 -q 'modprinc -policy lockout_policy "+
            			princName+"';clush -a 'usermod -L " + princName + "'";
//            	System.out.println(cmd);
            	ssh.execCommand(cmd);
            	InputStream stdoutStream = new StreamGobbler(ssh.getStdout());
            	BufferedReader stdoutReader = new BufferedReader(new InputStreamReader(stdoutStream));
            	String line="";
            	while((line = stdoutReader.readLine()) != null){
            		try {
						securityDao.updateStatus(hostname, port, princName, 0);
					} catch (SQLException e) {
						// TODO Auto-generated catch block
						e.printStackTrace();
					}
            		return true;
            	}
            	stdoutReader.close();
            	InputStream stderrStream = new StreamGobbler(ssh.getStderr());
            	BufferedReader stderrReader = new BufferedReader(new InputStreamReader(stderrStream));
            	while((line = stderrReader.readLine()) != null){
            		System.out.println(line);
            	}
            	stderrReader.close();
            }
            ssh.close();
            conn.close();
            return false;
        }
        
        //解锁principal
        public boolean unlockPrinc(String hostname,String port,String username,String password,String princName,String ipStr) throws IOException {
            Connection conn= new Connection(hostname);
            Session ssh = null;
            conn.connect();
            boolean isconn=conn.authenticateWithPassword(username, password);
            if(!isconn){  
                System.out.println("用户名称或者是密码不正确");  
            }else{
            	System.out.println("已经连接OK");
            	ssh=conn.openSession();
//	        	ssh.execCommand("sh /home/iie/kerberos-util/addprinc.sh "+princName);
            	String cmd = "kadmin -p cloudera-scm/admin -w 111111 -q 'modprinc -unlock " + 
            			princName + "';clush -a 'usermod -U " +
            			princName + "'";
//            	System.out.println(cmd);
            	ssh.execCommand(cmd);
            	InputStream stdoutStream = new StreamGobbler(ssh.getStdout());
            	BufferedReader stdoutReader = new BufferedReader(new InputStreamReader(stdoutStream));
            	String line="";
            	while((line = stdoutReader.readLine()) != null){
            		try {
						securityDao.updateStatus(hostname, port, princName, 1);
					} catch (SQLException e) {
						// TODO Auto-generated catch block
						e.printStackTrace();
					}
            		return true;
            	}
            	stdoutReader.close();
            	InputStream stderrStream = new StreamGobbler(ssh.getStderr());
            	BufferedReader stderrReader = new BufferedReader(new InputStreamReader(stderrStream));
            	while((line = stderrReader.readLine()) != null){
            		System.out.println(line);
            	}
            	stderrReader.close();
            }
            ssh.close();
            conn.close();
            return false;
        }
        
        //为principal生成keytab
        public boolean addKey(String hostname,String username,String password,String princName) throws IOException {
            Connection conn= new Connection(hostname);
            Session ssh = null;
            conn.connect();
            boolean isconn=conn.authenticateWithPassword(username, password);
            if(!isconn){  
                System.out.println("用户名称或者是密码不正确");  
            }else{
                System.out.println("已经连接OK");
                ssh=conn.openSession();
                ssh.execCommand("kadmin.local -q 'xst -k /home/"+princName+"/"+princName+".keytab -norandkey "+princName+"';chown "+princName+":"+princName+" /home/"+princName+"/"+princName+".keytab");
                InputStream is=new StreamGobbler(ssh.getStdout());
                BufferedReader br=new BufferedReader(new InputStreamReader(is));
                String line="";
                while((line=br.readLine())!=null){
//					System.out.println(line);
                    if(line.substring(line.length()-8).equals(".keytab.")){
                        br.close();
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
        
        //为principal改密码
        public boolean changePw(String hostname,String username,String password,String princName,String newPassWd) throws IOException {
            Connection conn= new Connection(hostname);
            Session ssh = null;
            conn.connect();
            boolean isconn=conn.authenticateWithPassword(username, password);
            if(!isconn){  
                System.out.println("用户名称或者是密码不正确");  
            }else{
                System.out.println("已经连接OK");
                ssh=conn.openSession();
                ssh.execCommand("kadmin -p cloudera-scm/admin -w 111111 -q 'cpw -pw "+newPassWd+" "+princName+"'");
                InputStream is=new StreamGobbler(ssh.getStdout());
                BufferedReader br=new BufferedReader(new InputStreamReader(is));
                String line="";
                while((line=br.readLine())!=null){
//					System.out.println(line);
                    if(line.substring(line.length()-8).equals("changed.")){
                        br.close();
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
        
        //删除principal
        public boolean delPrinc(String hostname,String username,String password,String princName) throws IOException {
            Connection conn= new Connection(hostname);
            Session ssh = null;
            conn.connect();
            boolean isconn=conn.authenticateWithPassword(username, password);
            if(!isconn){  
                System.out.println("用户名称或者是密码不正确");  
            }else{
                System.out.println("已经连接OK");
                ssh=conn.openSession();
//				ssh.execCommand("sh /home/iie/kerberos-util/delprinc.sh "+princName);
                ssh.execCommand("kadmin -p cloudera-scm/admin -w 111111 -q 'delprinc -force "+princName+"'");
                InputStream is=new StreamGobbler(ssh.getStdout());
                BufferedReader br=new BufferedReader(new InputStreamReader(is));
                String line="";
                while((line=br.readLine())!=null){
//                    System.out.println(line);
                    if(line.substring(line.length()-8).equals("deleted.")){
                        br.close();
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
        
        //返回principal列表
        public List<String> listPrincs(String hostname,String username,String password) throws IOException {
            Connection conn= new Connection(hostname);
            Session ssh = null;
            conn.connect();
            boolean isconn=conn.authenticateWithPassword(username, password);
            List<String> list=new ArrayList<String>();
            if(!isconn){  
                System.out.println("用户名称或者是密码不正确");  
            }else{
                System.out.println("已经连接OK");
                ssh=conn.openSession();
//				ssh.execCommand("sh /home/iie/kerberos-util/listprincs.sh");
                ssh.execCommand("kadmin -p cloudera-scm/admin -w 111111 -q 'listprincs'");
                InputStream is=new StreamGobbler(ssh.getStdout());
                BufferedReader br=new BufferedReader(new InputStreamReader(is));
                String line="";
                while((line=br.readLine())!=null){
                    list.add(line);
                }
            }
            ssh.close();
            conn.close();
            return list;
        }
        
        //验证principal或者登陆(keytab方式)
        public boolean kinitKey(String hostname,String username,String password,String princName) throws IOException {
            Connection conn= new Connection(hostname);
            Session ssh = null;
            conn.connect();
            boolean isconn=conn.authenticateWithPassword(username, password);
            if(!isconn){  
                System.out.println("用户名称或者是密码不正确");  
            }else{
                System.out.println("已经连接OK");
                ssh=conn.openSession();
                ssh.execCommand("kinit -kt /home/"+princName+"/"+princName+".keytab "+princName);
                InputStream stderr=new StreamGobbler(ssh.getStderr());
                BufferedReader stderrReader=new BufferedReader(new InputStreamReader(stderr));
                if(stderrReader.readLine()==null){
                    ssh.close();
                    conn.close();
                    return true;
                }else{
                    ssh.close();
                    conn.close();
                    return false;
                }
            }
            ssh.close();
            conn.close();
            return false;
        }
        
        //验证principal或者登陆(密码方式)
        public boolean kinitPw(String hostname,String username,String password,String princName,String passWd) throws IOException {
            Connection conn= new Connection(hostname);
            Session ssh = null;
            conn.connect();
            boolean isconn=conn.authenticateWithPassword(username, password);
            if(!isconn){  
                System.out.println("用户名称或者是密码不正确");  
            }else{
                System.out.println("已经连接OK");
                ssh=conn.openSession();
                ssh.execCommand("echo "+passWd+"|kinit "+princName);
                InputStream stderr=new StreamGobbler(ssh.getStderr());
                BufferedReader stderrReader=new BufferedReader(new InputStreamReader(stderr));
                String line="";
                if(stderrReader.readLine()==null){
                    ssh.close();
                    conn.close();
                    return true;
                }else{
                    ssh.close();
                    conn.close();
                    return false;
                }
            }
            ssh.close();
            conn.close();
            return false;
        }
        
        //销毁当前登陆
        public void kdestroy(String hostname,String username,String password) throws IOException{
            Connection conn= new Connection(hostname);
            Session ssh = null;
            conn.connect();
            boolean isconn=conn.authenticateWithPassword(username, password);
            if(!isconn){  
                System.out.println("用户名称或者是密码不正确");  
            }else{
                System.out.println("已经连接OK");
                ssh=conn.openSession();
                ssh.execCommand("kdestroy");
            }
            ssh.close();
            conn.close();
        }
        public static void main(String[] args) {
            SecurityPrinc sp=new SecurityPrinc();
            try {
                // boolean t=sp.delPrinc("10.208.133.162","root","iie@509!","victor111");
            	boolean t = sp.unlockPrinc("10.208.133.160", "3306", "root", "iie@509!", "test99","t161");
                System.out.println(t);
            } catch (IOException e) {
                // TODO Auto-generated catch block
                e.printStackTrace();
            }
            
        }
}