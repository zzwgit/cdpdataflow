package io.infinivision.flink.entity;

import io.infinivision.flink.client.LocalExecutorExtend;
import org.apache.flink.table.client.gateway.Executor;
import org.apache.flink.table.client.gateway.SessionContext;

public class ContextInfoEntity {

    private CheckPointEntity checkPointEntity;

    private LocalExecutorExtend executor;

    private SessionContext sessionContext;

    private String sql;

    public ContextInfoEntity() {
    }

    public ContextInfoEntity(CheckPointEntity checkPointEntity) {
        this.checkPointEntity = checkPointEntity;
    }

    public ContextInfoEntity(CheckPointEntity checkPointEntity, LocalExecutorExtend executor, SessionContext sessionContext, String sql) {
        this.checkPointEntity = checkPointEntity;
        this.executor = executor;
        this.sessionContext = sessionContext;
        this.sql = sql;
    }

    public CheckPointEntity getCheckPointEntity() {
        return checkPointEntity;
    }

    public void setCheckPointEntity(CheckPointEntity checkPointEntity) {
        this.checkPointEntity = checkPointEntity;
    }

    public LocalExecutorExtend getExecutor() {
        return executor;
    }

    public void setExecutor(LocalExecutorExtend executor) {
        this.executor = executor;
    }

    public SessionContext getSessionContext() {
        return sessionContext;
    }

    public void setSessionContext(SessionContext sessionContext) {
        this.sessionContext = sessionContext;
    }

    public String getSql() {
        return sql;
    }

    public void setSql(String sql) {
        this.sql = sql;
    }
}
