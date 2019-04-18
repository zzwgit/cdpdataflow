package io.infinivision.flink.handler;

import io.infinivision.flink.entity.ContextInfoEntity;
import org.apache.commons.lang3.StringUtils;
import org.apache.flink.table.client.cli.Client;
import org.apache.flink.table.client.cli.SqlCommandParser;

import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;
import java.util.Optional;

public class SqlHandler {

    private static SqlHandler INSTANCE = null;

    public static synchronized SqlHandler getInstance() {
        if (INSTANCE == null) {
            INSTANCE = new SqlHandler();
        }
        return INSTANCE;
    }

    public void handleSql(ContextInfoEntity contextInfo) throws NoSuchMethodException {

        Client cli = new Client(contextInfo.getSessionContext(), contextInfo.getExecutor());
//        Method method = cli.getClass().getDeclaredMethod("callCommand", SqlCommandParser.SqlCommandCall.class);
//        method.setAccessible(true);

        String sql = contextInfo.getSql();

        for (String s : StringUtils.split(sql, ";")) {

            Optional<SqlCommandParser.SqlCommandCall> cmdCall = SqlCommandParser.parse(s);
//                method.invoke(cli, cmdCall.get());
            cli.callCommand(cmdCall.get());
            System.err.println("--------------------finished execute---------------------");
        }

    }
}
