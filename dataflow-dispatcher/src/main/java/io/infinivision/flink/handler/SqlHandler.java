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

    public String handleSql(ContextInfoEntity contextInfo) throws Exception {

        Client cli = new Client(contextInfo.getSessionContext(), contextInfo.getExecutor());

        String sql = contextInfo.getSql();
        if (StringUtils.isBlank(sql)) {
            throw new Exception("sql context is empty!");
        }

        StringBuffer sb = new StringBuffer();
        for (String s : sql.split("(;\r\n|;\r|;\n)")) {
            Optional<SqlCommandParserExtend.SqlCommandCall> cmdCall = SqlCommandParserExtend.parse(s);
            if (!cmdCall.isPresent()) {
                System.err.println("error in execute:" + s);
                System.err.println("parse is :" + cmdCall.toString());
            }
            String result = cli.callCommand(cmdCall.get());
            sb.append(result);
        }
        return sb.toString();
    }
}
