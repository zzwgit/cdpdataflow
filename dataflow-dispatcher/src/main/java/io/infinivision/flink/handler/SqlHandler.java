package io.infinivision.flink.handler;

import io.infinivision.flink.entity.ContextInfoEntity;
import org.apache.commons.lang3.StringUtils;
import org.apache.flink.table.client.cli.Client;
import org.apache.flink.table.client.cli.SqlCommandParserExtend;

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
//        Method method = cli.getClass().getDeclaredMethod("callCommand", SqlCommandParserExtend.SqlCommandCall.class);
//        method.setAccessible(true);

        String sql = contextInfo.getSql();

        for (String s : StringUtils.split(sql, ";")) {

            Optional<SqlCommandParserExtend.SqlCommandCall> cmdCall = SqlCommandParserExtend.parse(s);
//                method.invoke(cli, cmdCall.get());
            if (!cmdCall.isPresent()) {
                System.err.println("error in execute:" + s);
                System.err.println("parse is :" + cmdCall.toString());
            }
            cli.callCommand(cmdCall.get());
        }

    }
}
