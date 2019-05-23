package io.infinivision.flink.core;

import com.alibaba.fastjson.JSON;
import com.google.common.collect.Maps;
import io.infinivision.flink.client.LocalExecutorExtend;
import io.infinivision.flink.entity.CheckPointEntity;
import io.infinivision.flink.entity.ContextInfoEntity;
import io.infinivision.flink.handler.*;
import io.infinivision.flink.parser.OptionsParser;
import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.ParseException;
import org.apache.commons.lang3.StringUtils;
import org.apache.flink.table.client.cli.CliOptionsParser;
import org.apache.flink.table.client.config.Environment;
import org.apache.flink.table.client.gateway.SessionContext;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IOUtils;

import java.io.*;
import java.lang.reflect.InvocationTargetException;
import java.net.URI;
import java.net.URL;
import java.util.Arrays;
import java.util.List;
import java.util.Map;
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

    public String dispatching(String[] args)  throws Exception {

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

        //checkpoint
        String checkpointPath = commandLine.getOptionValue(OptionsParser.OPTION_CONF_PATH.getOpt());
        CheckPointEntity checkPointEntity = JSON.parseObject(readFile(checkpointPath), CheckPointEntity.class);
//        String interval = commandLine.getOptionValue(OptionsParser.OPTION_CP_INTERVALTIME.getOpt(), "-1");
//        CheckPointEntity checkPointEntity = new CheckPointEntity(Long.parseLong(interval), commandLine.getOptionValue(OptionsParser.OPTION_CP_MODE.getOpt()), commandLine.getOptionValue(OptionsParser.OPTION_CP_STATEBACKEND.getOpt()), commandLine.getOptionValue(OptionsParser.OPTION_CP_STATECHECKPOINTSDIR.getOpt()));

        //fromSavepoint
        String fromSavepoint = commandLine.getOptionValue(OptionsParser.OPTION_FROMSAVEPOINT.getOpt());
        //sqlpath
        String sqlPath = commandLine.getOptionValue(OptionsParser.OPTION_SQLPATH.getOpt());

        // SessionContext
        Environment sessionEnv = null == environment ? new Environment() : Environment.parse(environment);

        Map<String, Object> sp = Maps.newHashMap();
//        sp.put("-m", "yarn-cluster");
//        sp.put("-yn", "1");
//        sp.put("-ys", "1");
//        sp.put("-yjm", "2048");
//        sp.put("-ytm", "2048");

        // fromsavepoints (ps:ConfigUtil.normalizeYaml中会将key转换为小写)
        if (StringUtils.isNotEmpty(fromSavepoint)) {
            sp.put("-s", fromSavepoint);
            sp.put("-n", "true");
        }
        sp.putAll(sessionEnv.getDeployment().asMap());
        sessionEnv.setDeployment(sp);

        SessionContext sessionContext = new SessionContext(sessionId, sessionEnv);

        // Executor
//        LocalExecutor executor = new LocalExecutor(defaults, jars, libDirs);
        LocalExecutorExtend executor = new LocalExecutorExtend(defaults, jars, libDirs);
        executor.validateSession(sessionContext);

        // sql
        String sql = trimSql(readFile(sqlPath));

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

        if(StringUtils.isEmpty(path)){
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
