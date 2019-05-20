package io.infinivision.flink;

import com.jcraft.jsch.Session;
import io.infinivision.flink.utils.SSHUtils;

public class CallSShYarn {

    public static void main(String[] args){
        try {
            SSHUtils.DestHost host = new SSHUtils.DestHost("172.19.0.16", "root", "Welcome123@");

            String stdout = "";

            StringBuilder sb = new StringBuilder();
            sb.append("export FLINK_CONF_DIR=/bigdata/flink-yarn-1.5.1/conf ");
//            sb.append("export HADOOP_HOME=/bigdata/hadoop-2.7.5 ");
            sb.append("&& ");
//            sb.append("java -Dsaffron.default.charset=UTF-16LE -Dsaffron.default.nationalcharset=UTF-16LE -cp ");
            sb.append("java -cp ");
            sb.append("/bigdata/flink-yarn-1.5.1/lib/log4j-1.2.17.jar:");
            sb.append("/bigdata/flink-yarn-1.5.1/lib/slf4j-log4j12-1.7.7.jar:");
            sb.append("/bigdata/flink-yarn-1.5.1/lib/flink-dist_2.11-1.5.1.jar:");
            sb.append("/bigdata/flink-yarn-1.5.1/lib/flink-shaded-hadoop2-uber-1.5.1.jar:");
            sb.append("/bigdata/flink-yarn-1.5.1/lib/flink-python_2.11-1.5.1.jar:");
            sb.append("/bigdata/flink-yarn-1.5.1/opt/sql-client/flink-sql-client-1.5.1.jar:");
//            sb.append("/bigdata/flink-1.5.1/lib/flink-cep_2.11-1.5.1.jar:");
            sb.append("/bigdata/hadoop-2.7.5/etc/hadoop:");

            sb.append("/opt/apps/zf/blink-sql/dispatcher-3.0.jar io.infinivision.flink.core.Dispatcher ");
            sb.append("run ");
            sb.append("-s test-zf ");
            sb.append("-d /bigdata/flink-yarn-1.5.1/conf/sql-client-defaults.yaml ");
            sb.append("-l /bigdata/flink-yarn-1.5.1/opt ");
            sb.append("-l /bigdata/flink-yarn-1.5.1/opt/connectors ");
            sb.append("-l /bigdata/flink-yarn-1.5.1/opt/yarn-shuffle ");
            sb.append("-l /bigdata/flink-yarn-1.5.1/opt/connectors/kafka011 ");
            sb.append("-sqlPath /opt/apps/zf/blink-sql/event_streaming_pg.sql ");

//            sb.append("-intervalTime 10000 ");
//            sb.append("-mode EXACTLY_ONCE ");
//            sb.append("-stateBackend filesystem ");
//            sb.append("-stateCheckpointsDir hdfs:///flink-checkpoints/test_join_pg_s ");

//            sb.append("-fromSavepoint hdfs:///flink-checkpoints/test_join_pg_s/5395ccb928083875b088b1d640e69266/chk-3 ");


            Session shellSession = SSHUtils.getJSchSession(host);
            stdout = SSHUtils.execCommandByJSch(shellSession,sb.toString());
            shellSession.disconnect();

            System.out.println("end:"+stdout);
        } catch (Exception e) {
            e.printStackTrace();
        }
    }
}
