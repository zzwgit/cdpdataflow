package io.infinivision.flink;

import com.jcraft.jsch.Session;
import io.infinivision.flink.utils.SSHUtils;

public class CallSSh {

    public static void main(String[] args){
        try {
            SSHUtils.DestHost host = new SSHUtils.DestHost("172.19.0.16", "root", "Welcome123@");

            String stdout = "";

            StringBuilder sb = new StringBuilder();
            sb.append("export FLINK_CONF_DIR=/bigdata/flink-1.5.1/conf ");
            sb.append("&& ");
//            sb.append("java -Dsaffron.default.charset=UTF-16LE -Dsaffron.default.nationalcharset=UTF-16LE -cp ");
            sb.append("java -cp ");
            sb.append("/bigdata/flink-1.5.1/lib/flink-cep_2.11-1.5.1.jar:");
            sb.append("/bigdata/flink-1.5.1/lib/flink-python_2.11-1.5.1.jar:");
            sb.append("/bigdata/flink-1.5.1/lib/flink-shaded-hadoop2-uber-1.5.1.jar:");
            sb.append("/bigdata/flink-1.5.1/lib/log4j-1.2.17.jar:");
            sb.append("/bigdata/flink-1.5.1/lib/slf4j-log4j12-1.7.7.jar:");
            sb.append("/bigdata/flink-1.5.1/lib/flink-dist_2.11-1.5.1.jar:");
            sb.append("/bigdata/flink-1.5.1/opt/sql-client/flink-sql-client-1.5.1.jar:");
            sb.append("/bigdata/hadoop-2.7.5/etc/hadoop:");

            sb.append("/opt/apps/zf/blink-sql/dispatcher-1.0.jar io.infinivision.flink.core.Dispatcher ");
            sb.append("-s test ");
            sb.append("-d /bigdata/flink-1.5.1/conf/sql-client-defaults.yaml ");
            sb.append("-l /bigdata/flink-1.5.1/opt ");
            sb.append("-l /bigdata/flink-1.5.1/opt/connectors ");
            sb.append("-sqlPath /opt/apps/zf/blink-sql/01_cep.sql ");

            Session shellSession = SSHUtils.getJSchSession(host);
//            stdout = SSHUtils.execCommandByJSch(shellSession, "cd /opt/apps/zf/blink-sql");
//            stdout = SSHUtils.execCommandByJSch(shellSession, "export FLINK_CONF_DIR=/bigdata/flink-1.5.1/conf&&echo $FLINK_CONF_DIR");
//            stdout = SSHUtils.execCommandByJSch(shellSession, "export FLINK_CONF_DIR=/bigdata/flink-1.5.1/conf&&java -cp /opt/apps/zf/blink-sql/blinktest-0.0.1-SNAPSHOT.jar com.zf.client.SubmitJarUtil -s test -u /opt/apps/zf/blink-sql/test.sql -d /bigdata/flink-1.5.1/conf/sql-client-defaults.yaml -l /bigdata/flink-1.5.1/lib -l /bigdata/flink-1.5.1/opt -l /bigdata/flink-1.5.1/opt/connectors -l /bigdata/flink-1.5.1/opt/sql-client");
//            stdout = SSHUtils.execCommandByJSch(shellSession, "export FLINK_CONF_DIR=/bigdata/flink-1.5.1/conf&&java -cp /opt/apps/zf/blink-sql/client-jar-1.0-SNAPSHOT.jar com.zf.client.SubmitJarUtil -s test -u /opt/apps/zf/blink-sql/test.sql -d /bigdata/flink-1.5.1/conf/sql-client-defaults.yaml -e /bigdata/flink-1.5.1/conf/sql-client-defaults.yaml -l /bigdata/flink-1.5.1/lib -l /bigdata/flink-1.5.1/opt -l /bigdata/flink-1.5.1/opt/connectors -l /bigdata/flink-1.5.1/opt/sql-client");
            stdout = SSHUtils.execCommandByJSch(shellSession,sb.toString());
            shellSession.disconnect();

            System.out.println("end:"+stdout);
        } catch (Exception e) {
            e.printStackTrace();
        }
    }
}
