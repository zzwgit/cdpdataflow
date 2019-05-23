package io.infinivision.flink.athenax.backend.api;

import com.fasterxml.jackson.annotation.JsonProperty;

import java.util.Collections;
import java.util.List;

public class JobRequest {

    //    会话名称        -s test
    @JsonProperty("s")
    private String session = null;

    //    默认环境配置    -d /bigdata/flink-1.5.1/conf/sql-client-defaults.yaml
    @JsonProperty("d")
    private String defaults = null;

    //    指定环境配置    -e /bigdata/flink-1.5.1/conf/sql-client-defaults.yaml
    @JsonProperty("e")
    private String environment = null;

    //    自定义jar lib   -l /bigdata/flink-1.5.1/opt
    @JsonProperty("l")
    private List<String> library = Collections.emptyList();

    //    自定义jar       -j /bigdata/flink-1.5.1/opt/xxx.jar
    @JsonProperty("j")
    private List<String> jar = Collections.emptyList();

    //    配置文件位置    -confPath hdfs:///flink-sql-test-data/conf.json
    @JsonProperty("confPath")
    private String confPath = null;

    //    sql文件位置     -sqlPath hdfs:///flink-sql-test-data/test_kafka_hdfs.sql
    @JsonProperty("sqlPath")
    private String sqlPath = null;

    //    从保存点恢复    -fromSavepoint hdfs:///flink-checkpoints/test_join_pg_s/5395ccb928083875b088b1d640e69266/chk-3
    @JsonProperty("fromSavepoint")
    private String fromSavepoint = null;

    public String getSession() {
        return session;
    }

    public void setSession(String session) {
        this.session = session;
    }

    public String getDefaults() {
        return defaults;
    }

    public void setDefaults(String defaults) {
        this.defaults = defaults;
    }

    public String getEnvironment() {
        return environment;
    }

    public void setEnvironment(String environment) {
        this.environment = environment;
    }

    public List<String> getLibrary() {
        return library;
    }

    public void setLibrary(List<String> library) {
        this.library = library;
    }

    public List<String> getJar() {
        return jar;
    }

    public void setJar(List<String> jar) {
        this.jar = jar;
    }

    public String getConfPath() {
        return confPath;
    }

    public void setConfPath(String confPath) {
        this.confPath = confPath;
    }

    public String getSqlPath() {
        return sqlPath;
    }

    public void setSqlPath(String sqlPath) {
        this.sqlPath = sqlPath;
    }

    public String getFromSavepoint() {
        return fromSavepoint;
    }

    public void setFromSavepoint(String fromSavepoint) {
        this.fromSavepoint = fromSavepoint;
    }
}
