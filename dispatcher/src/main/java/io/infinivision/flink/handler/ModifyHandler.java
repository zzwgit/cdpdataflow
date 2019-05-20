package io.infinivision.flink.handler;

import io.infinivision.flink.entity.ContextInfoEntity;
import org.apache.commons.lang3.StringUtils;
import org.apache.flink.table.client.cli.Client;
import org.apache.flink.table.client.cli.SqlCommandParserExtend;

import java.util.Optional;

public class ModifyHandler {

    private static ModifyHandler INSTANCE = null;

    public static synchronized ModifyHandler getInstance() {
        if (INSTANCE == null) {
            INSTANCE = new ModifyHandler();
        }
        return INSTANCE;
    }

    public void modify(ContextInfoEntity contextInfo, String[] params) {

        Client cli = new Client(contextInfo.getSessionContext(), contextInfo.getExecutor());

        String requestBody = contextInfo.getSql();
//        String sql = "{\"jobid\":\"a07a1402bce5062e13e7a454c7c65725\",\"parallelism\":\"2\",\"vertex-parallelism-resource\":{\"cbc357ccb763df2852fee8c4fc7d55f2\":{\"parallelism\":1}}}";
        cli.callModify(requestBody);


    }
}
