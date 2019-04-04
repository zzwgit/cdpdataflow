package io.infinivision.flink.entity;

import org.apache.flink.table.client.gateway.Executor;
import org.apache.flink.table.client.gateway.SessionContext;

public class ContextInfoEntity {

    private CheckPointEntity checkPointEntity;

    private Executor executor;

    private SessionContext sessionContext;

    private String sql;

    public ContextInfoEntity() {
    }

    public ContextInfoEntity(CheckPointEntity checkPointEntity) {
        this.checkPointEntity = checkPointEntity;
    }

    public ContextInfoEntity(CheckPointEntity checkPointEntity, Executor executor, SessionContext sessionContext, String sql) {
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

    public Executor getExecutor() {
        return executor;
    }

    public void setExecutor(Executor executor) {
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
