package com.bigdata.spark;

import com.jcraft.jsch.*;

import java.io.BufferedReader;
import java.io.InputStreamReader;
import java.util.Vector;

/**
 * Created by xdf on 2018/8/9.
 */
public class SSHCommandExecutor {
    private String ipAddress;
    private String username;
    private String password;

    public static final int DEFAULT_SSH_PORT = 22;

    private Vector<String> stdout;

    public SSHCommandExecutor(final String ipAddress, final String username, final String password) {
        this.ipAddress = ipAddress;
        this.username = username;
        this.password = password;
        stdout = new Vector<String>();
    }

    public int execute(final String command) {
        int returnCode = 0;
        JSch jsch = new JSch();
        MyUserInfo userInfo = new MyUserInfo();

        try {
            // Create and connect session.
            Session session = jsch.getSession(username, ipAddress, DEFAULT_SSH_PORT);
            session.setPassword(password);
            session.setUserInfo(userInfo);
            session.connect();

            // Create and connect channel.
            Channel channel = session.openChannel("exec");
            ((ChannelExec) channel).setCommand(command);
            channel.setInputStream(null);
            BufferedReader input = new BufferedReader(new InputStreamReader(channel
                    .getInputStream()));
            channel.connect();
            System.out.println("The remote command is: " + command);

            // Get the output of remote command.
            String line;
            while ((line = input.readLine()) != null) {
                stdout.add(line);
            }
            input.close();

            // Get the return code only after the channel is closed.
            if (channel.isClosed()) {
                returnCode = channel.getExitStatus();
            }

            // Disconnect the channel and session.
            channel.disconnect();
            session.disconnect();
        } catch (JSchException e) {
            e.printStackTrace();
        } catch (Exception e) {
            e.printStackTrace();
        }
        return returnCode;
    }

    public Vector<String> getStandardOutput() {
        return stdout;
    }

    public static void main(final String [] args) {
        SSHCommandExecutor sshExecutor = new SSHCommandExecutor("10.10.86.192", "bd-warehouse", "bd-warehouse");
        String task_id = "20180809001";
        String cmd = "impala-shell -i bwcmrm2.heracles.sohuno.com:25003 -d bd_wh -u bd-warehouse -q \"alter table bd_wh.push_batch_data add if not exists partition(task_id='"+task_id+"');\"";
        sshExecutor.execute(cmd);

        Vector<String> stdout = sshExecutor.getStandardOutput();
        for (String str : stdout) {
            System.out.println(str);
        }
    }
}
