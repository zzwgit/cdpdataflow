package io.infinivision.flink.core;

import com.google.common.collect.Maps;
import io.infinivision.flink.client.LocalExecutorExtend;
import io.infinivision.flink.entity.CheckPointEntity;
import io.infinivision.flink.entity.ContextInfoEntity;
import io.infinivision.flink.handler.EnvHandler;
import io.infinivision.flink.handler.SqlHandler;
import io.infinivision.flink.parser.OptionsParser;
import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.ParseException;
import org.apache.commons.lang3.StringUtils;
import org.apache.flink.table.client.cli.CliOptionsParser;
import org.apache.flink.table.client.config.Environment;
import org.apache.flink.table.client.gateway.Executor;
import org.apache.flink.table.client.gateway.SessionContext;
import org.apache.flink.table.client.gateway.local.LocalExecutor;

import java.io.*;
import java.lang.reflect.InvocationTargetException;
import java.net.URL;
import java.util.List;
import java.util.Map;
import java.util.regex.Pattern;

public class Dispatcher {

    private EnvHandler envHandler = EnvHandler.getInstance();

    private SqlHandler sqlHandler = SqlHandler.getInstance();

    public static void main(String[] args) throws IOException, NoSuchMethodException, InvocationTargetException, IllegalAccessException, ParseException {

        Dispatcher util = new Dispatcher();

        ContextInfoEntity contextInfo = util.getContextInfoEntity(args);

        util.envHandler.setAttributes(contextInfo);

        util.sqlHandler.handleSql(contextInfo);
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
        String interval = commandLine.getOptionValue(OptionsParser.OPTION_CP_INTERVALTIME.getOpt(), "-1");
        CheckPointEntity checkPointEntity = new CheckPointEntity(Long.parseLong(interval), commandLine.getOptionValue(OptionsParser.OPTION_CP_MODE.getOpt()), commandLine.getOptionValue(OptionsParser.OPTION_CP_STATEBACKEND.getOpt()), commandLine.getOptionValue(OptionsParser.OPTION_CP_STATECHECKPOINTSDIR.getOpt()));

        //fromSavepoint
        String fromSavepoint = commandLine.getOptionValue(OptionsParser.OPTION_FROMSAVEPOINT.getOpt());
        //sqlpath
        String sqlPath = commandLine.getOptionValue(OptionsParser.OPTION_SQLPATH.getOpt());

        // SessionContext
        Environment sessionEnv = null == environment ? new Environment() : Environment.parse(environment);

        // fromsavepoints (ps:ConfigUtil.normalizeYaml中会将key转换为小写)
        if (StringUtils.isNotEmpty(fromSavepoint)) {
            Map<String, Object> sp = Maps.newHashMap();
            sp.put("-s", fromSavepoint);
            sp.put("-n", "true");
            sp.putAll(sessionEnv.getDeployment().asMap());
            sessionEnv.setDeployment(sp);
        }

        SessionContext sessionContext = new SessionContext(sessionId, sessionEnv);

        // Executor
//        LocalExecutor executor = new LocalExecutor(defaults, jars, libDirs);
        LocalExecutorExtend executor = new LocalExecutorExtend(defaults, jars, libDirs);
        executor.validateSession(sessionContext);

        // sql
        String sql = trimSql(readLocal(sqlPath));

        return new ContextInfoEntity(checkPointEntity, executor, sessionContext, sql);

    }

    public String readLocal(String path) {
        String encoding = "UTF-8";
        File file = new File(path);
        Long filelength = file.length();
        byte[] filecontent = new byte[filelength.intValue()];
        try {
            FileInputStream in = new FileInputStream(file);
            in.read(filecontent);
            in.close();
        } catch (FileNotFoundException e) {
            e.printStackTrace();
        } catch (IOException e) {
            e.printStackTrace();
        }
        try {
            return new String(filecontent, encoding);
        } catch (UnsupportedEncodingException e) {
            System.err.println("The OS does not support " + encoding);
            e.printStackTrace();
            return null;
        }
    }

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

//    public String readHdfs(String path) {
//
//        // String path = "hdfs://172.19.0.16:9000/flink-sql-test-data/test.sql";
//
//        Configuration conf = new Configuration();
//        FileSystem fs = null;
//        InputStream in = null;
//        StringBuffer sb = new StringBuffer();
//
//        try {
//            fs = FileSystem.get(URI.create(path), conf);
//            in = fs.open(new Path(path));
//
//            byte[] ioBuffer = new byte[100];
//            int byteread = 0;
//            while ((byteread = in.read(ioBuffer)) != -1) {
//                sb.append(new String(ioBuffer, 0, byteread, "UTF-8"));
//            }
//            return sb.toString();
//        } catch (IOException e) {
//            e.printStackTrace();
//            return null;
//        } finally {
//            IOUtils.closeStream(fs);
//            IOUtils.closeStream(in);
//        }
//
//    }

}
