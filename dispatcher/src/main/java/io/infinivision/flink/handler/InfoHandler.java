package io.infinivision.flink.handler;

import io.infinivision.flink.entity.ContextInfoEntity;
import org.apache.flink.table.client.cli.Client;

public class InfoHandler {

    private static InfoHandler INSTANCE = null;

    public static synchronized InfoHandler getInstance() {
        if (INSTANCE == null) {
            INSTANCE = new InfoHandler();
        }
        return INSTANCE;
    }

    public void info(ContextInfoEntity contextInfo, String[] params) {

        Client cli = new Client(contextInfo.getSessionContext(), contextInfo.getExecutor());

        String requestBody = contextInfo.getSql();
        cli.callGetJobStatus(requestBody);


    }
}
