package com.bigdata.hive.quanwen;

import ch.ethz.ssh2.Connection;
import ch.ethz.ssh2.Session;
import ch.ethz.ssh2.StreamGobbler;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;

public class QwCount {
	
	public QwCount(){
		
	}
	
	public long chaxun(String hostname,String username,String password,String database,String table) throws IOException {
        Connection conn= new Connection(hostname);
        Session ssh = null;
        conn.connect();
        boolean isconn=conn.authenticateWithPassword(username, password);
        if(!isconn){  
            System.out.println("用户名或密码错误");  
        }else{
            System.out.println("已经连接ok");
            ssh=conn.openSession();
            ssh.execCommand("clush -a 'sh /var/lib/dc_Monitor/dc_Monitor.sh "+database+".db "+table+" 2>> /var/lib/dc_Monitor/quanwen_err.log'");
            InputStream is=new StreamGobbler(ssh.getStdout());
			BufferedReader br=new BufferedReader(new InputStreamReader(is));
            String line="";
            int count=0;
            while((line=br.readLine())!=null){
                String[] num=line.split(" ");
                count=count+Integer.parseInt(num[1]);
            }
            return count;
        }
        ssh.close();
        conn.close();
        return 0;
    }
}
