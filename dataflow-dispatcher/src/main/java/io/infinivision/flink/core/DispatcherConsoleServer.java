package io.infinivision.flink.core;

import com.alibaba.fastjson.JSON;
import com.google.common.collect.Maps;
import io.infinivision.flink.client.LocalExecutorExtend;
import io.infinivision.flink.entity.CheckPointEntity;
import io.infinivision.flink.entity.ContextInfoEntity;
import io.infinivision.flink.handler.EnvHandler;
import io.infinivision.flink.handler.InfoHandler;
import io.infinivision.flink.handler.ModifyHandler;
import io.infinivision.flink.handler.SqlHandler;
import io.infinivision.flink.parser.OptionsParser;
import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.ParseException;
import org.apache.commons.collections.MapUtils;
import org.apache.commons.io.FileUtils;
import org.apache.commons.lang3.StringUtils;
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.databind.JsonNode;
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.databind.node.ObjectNode;
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.dataformat.yaml.YAMLFactory;
import org.apache.flink.table.client.cli.CliOptionsParser;
import org.apache.flink.table.client.config.Environment;
import org.apache.flink.table.client.gateway.SessionContext;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IOUtils;

import java.io.File;
import java.io.IOException;
import java.io.InputStream;
import java.lang.reflect.InvocationTargetException;
import java.net.URI;
import java.net.URL;
import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.regex.Pattern;

public class DispatcherConsoleServer {

    // actions
    private static final String ACTION_RUN = "run";
    private static final String ACTION_MODIFY = "modify";
    private static final String ACTION_INFO = "info";

    private EnvHandler envHandler = EnvHandler.getInstance();

    private SqlHandler sqlHandler = SqlHandler.getInstance();

    private ModifyHandler modifyHandler = ModifyHandler.getInstance();

    private InfoHandler infoHandler = InfoHandler.getInstance();

    //命令行方式时的入口
    public static void main(String[] args) throws Exception {
        DispatcherConsoleServer dispatcher = new DispatcherConsoleServer();
        dispatcher.dispatching(args);
    }

    public String dispatching(String[] args) throws Exception {

        // get action
        String action = args[0];
        System.out.println("-------------------------------------------" + action);

        // remove action from parameters
        final String[] params = Arrays.copyOfRange(args, 1, args.length);

        ContextInfoEntity contextInfo = this.getContextInfoEntity(params);

        switch (action) {
            case ACTION_RUN:
                this.envHandler.setAttributes(contextInfo);
                return this.sqlHandler.handleSql(contextInfo);

            case ACTION_MODIFY:
                this.modifyHandler.modify(contextInfo, params);
                return null;

            case ACTION_INFO:
                this.infoHandler.info(contextInfo, params);
                break;
            default:
                System.out.println("-----------------do nothing--------------------------");
                return null;
        }
        return null;

    }


    private ContextInfoEntity getContextInfoEntity(String[] args) throws IOException, InvocationTargetException, IllegalAccessException, ParseException {

        CommandLine commandLine = OptionsParser.argsToCommandLine(args);
        //sessionId
        String sessionId = commandLine.getOptionValue(CliOptionsParser.OPTION_SESSION.getOpt());
        // DEFAULTS
        URL defaults = OptionsParser.checkUrl(commandLine, CliOptionsParser.OPTION_DEFAULTS);
        // Environment
        URL environment = OptionsParser.checkUrl(commandLine, CliOptionsParser.OPTION_ENVIRONMENT);
        //jars
        List<URL> jars = OptionsParser.checkUrls(commandLine, CliOptionsParser.OPTION_JAR);
        //libs
        List<URL> libDirs = OptionsParser.checkUrls(commandLine, CliOptionsParser.OPTION_LIBRARY);
        //sqlpath
        String sqlPath = commandLine.getOptionValue(OptionsParser.OPTION_SQLPATH.getOpt());
        String sqlFileName = sqlPath.substring(sqlPath.lastIndexOf('/') + 1);
        // create SessionContext by user define (environment)
        ObjectMapper mapper = new ObjectMapper(new YAMLFactory());
        JsonNode enTree = mapper.readTree(environment);
        JsonNode deploymentNode = enTree.get("deployment");
        // 尊重用户的选择
        if (deploymentNode.get("ynm") == null) {
            ((ObjectNode) deploymentNode).put("ynm", sqlFileName);
        }
        Environment sessionEnv = null == environment ? new Environment() :
                Environment.parse(mapper.writeValueAsString(enTree));
        Map<String, String> deploymentMap = sessionEnv.getDeployment().asMap();

        //set checkpoint by read from environment
        Long intervalTime = MapUtils.getLong(deploymentMap, "checkpoint.intervalTime", -1L);
        String mode = MapUtils.getString(deploymentMap, "checkpoint.mode", "");
        String stateBackend = MapUtils.getString(deploymentMap, "checkpoint.stateBackend", "");
        String stateCheckpointsDir = MapUtils.getString(deploymentMap, "checkpoint.stateCheckpointsDir", "");
        CheckPointEntity checkPointEntity = new CheckPointEntity(intervalTime, mode, stateBackend, stateCheckpointsDir);

        //set fromSavepoint by read from environment
        String fromSavepoint = MapUtils.getString(deploymentMap, "fromSavepoint", "");
//        String fromSavepoint = commandLine.getOptionValue(OptionsParser.OPTION_FROMSAVEPOINT.getOpt());
//        Map<String, Object> sp = Maps.newHashMap();
        // fromsavepoints (ps:ConfigUtil.normalizeYaml中会将key转换为小写)
//        if (StringUtils.isNotEmpty(fromSavepoint)) {
//            sp.put("-s", fromSavepoint);
//            sp.put("-n", "true");
//        }
//        sp.putAll(sessionEnv.getDeployment().asMap());
//        sessionEnv.setDeployment(sp);

        SessionContext sessionContext = new SessionContext(sessionId, sessionEnv);

        // Executor
//        LocalExecutor executor = new LocalExecutor(defaults, jars, libDirs);
        Properties additionFlinkConfiguration = new Properties();
        additionFlinkConfiguration.put("metrics.reporter.promgateway.jobName", sqlFileName);
        LocalExecutorExtend executor = new LocalExecutorExtend(defaults, jars, libDirs, additionFlinkConfiguration);
        executor.validateSession(sessionContext);

        // sql
        String sql = trimSql(readFile(sqlPath));

        // 尊重用户的选择, attention, 需要严格的正则，因为有可能表的字段存在commit单词
        if (!sql.toLowerCase().matches(".*\\bcommit\\b\\s\\+\\S\\+\\s*;.*")) {
            sql += ";\nCOMMIT " + sqlFileName;
        }

        return new ContextInfoEntity(checkPointEntity, executor, sessionContext, sql);

    }

    /**
     * 读取sql文件，包括本地及hdfs文件
     *
     * @param path The path of the file, as a URI (e.g., "file:///some/local/file" or "hdfs://host:port/file/path").
     *             hdfs://172.19.0.16:9000/flink-sql-test-data/test.sql
     *             file:///D:/test-files/sql/event_streaming_pg.sql
     * @return
     */
    public String readFile(String path) {

        if (StringUtils.isEmpty(path)) {
            return null;
        }

        FileSystem fs = null;
        InputStream in = null;
        StringBuffer sb = new StringBuffer();

        try {
            Configuration conf = new Configuration();
            fs = FileSystem.get(URI.create(path), conf);
            in = fs.open(new Path(path));

            byte[] ioBuffer = new byte[1024];
            int byteread = 0;
            while ((byteread = in.read(ioBuffer)) != -1) {
                sb.append(new String(ioBuffer, 0, byteread, "UTF-8"));
            }
            return sb.toString();
        } catch (Exception e) {
            e.printStackTrace();
            return null;
        } finally {
            IOUtils.closeStream(fs);
            IOUtils.closeStream(in);
        }

    }

    /**
     * 过滤sql中的所有注释内容
     *
     * @param sql
     * @return
     */
    public String trimSql(String sql) {
        //去除注释
        Pattern p = Pattern.compile("(?ms)('(?:''|[^'])*')|--.*?$|/\\*.*?\\*/|#.*?$|");
        sql = p.matcher(sql).replaceAll("$1");
        //去除结尾多余行
        sql = sql.trim();
        //去除结尾;
        if (sql.endsWith(";")) {
            sql = sql.substring(0, sql.length() - 1).trim();
        }
        return sql;
    }
}
