package io.infinivision.flink.utils;

import com.jcraft.jsch.ChannelExec;
import com.jcraft.jsch.JSch;
import com.jcraft.jsch.JSchException;
import com.jcraft.jsch.Session;
import org.apache.commons.io.IOUtils;

import java.io.IOException;
import java.io.InputStream;

public class SSHUtils {

    private static final String ENCODING = "UTF-8";

    public static Session getJSchSession(DestHost destHost) throws JSchException {
        JSch jsch = new JSch();

        Session session = jsch.getSession(destHost.getUsername(), destHost.getHost(), destHost.getPort());
        session.setPassword(destHost.getPassword());
        session.setConfig("StrictHostKeyChecking", "no");  // 第一次访问服务器时不用输入yes
        session.setTimeout(destHost.getTimeout());
        session.connect();

        return session;
    }

    public static String execCommandByJSch(DestHost destHost, String command) throws IOException, JSchException {
        return execCommandByJSch(destHost, command, ENCODING);
    }

    public static String execCommandByJSch(DestHost destHost, String command, String resultEncoding) throws IOException, JSchException {
        Session session = getJSchSession(destHost);
        String result = execCommandByJSch(session, command, resultEncoding);
        session.disconnect();

        return result;
    }

    public static String execCommandByJSch(Session session, String command) throws IOException, JSchException {
        return execCommandByJSch(session, command, ENCODING);
    }

    public static String execCommandByJSch(Session session, String command, String resultEncoding) throws IOException, JSchException {
        ChannelExec channelExec = (ChannelExec) session.openChannel("exec");
        InputStream in = channelExec.getInputStream();
        channelExec.setCommand(command);
        channelExec.setErrStream(System.err);
        channelExec.connect();

        String result = IOUtils.toString(in, resultEncoding);

        channelExec.disconnect();

        return result;
    }

    /**
     * 目标登录主机信息
     */
    public static class DestHost {
        private String host = "";
        private String username = "";
        private String password = "";
        private int port = 22;
        private int timeout = 60 * 60 * 1000;

        public DestHost(String host, String username, String password) {
            this(host, username, password, 22, 60 * 60 * 1000);
        }

        public DestHost(String host, String username, String password, int timeout) {
            this(host, username, password, 22, timeout);
        }

        public DestHost(String host, String username, String password, int port, int timeout) {
            this.host = host;
            this.username = username;
            this.password = password;
            this.port = port;
            this.timeout = timeout;
        }

        public String getHost() {
            return host;
        }

        public void setHost(String host) {
            this.host = host;
        }

        public String getUsername() {
            return username;
        }

        public void setUsername(String username) {
            this.username = username;
        }

        public String getPassword() {
            return password;
        }

        public void setPassword(String password) {
            this.password = password;
        }

        public int getPort() {
            return port;
        }

        public void setPort(int port) {
            this.port = port;
        }

        public int getTimeout() {
            return timeout;
        }

        public void setTimeout(int timeout) {
            this.timeout = timeout;
        }

    }

}
