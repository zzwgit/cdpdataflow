# dispatcher

1.任务提交   
1.1 通过console方式：
```
./flink-sql.sh run -s test -confPath hdfs:///flink-sql-test-data/conf.json -sqlPath hdfs:///flink-sql-test-data/test_kafka_hdfs.sql

参数定义：
会话名称        -s test 
默认环境配置    -d /bigdata/flink-1.5.1/conf/sql-client-defaults.yaml 
指定环境配置    -e /bigdata/flink-1.5.1/conf/sql-client-defaults.yaml 
自定义jar lib   -l /bigdata/flink-1.5.1/opt
自定义jar       -j /bigdata/flink-1.5.1/opt/xxx.jar
配置文件位置    -confPath hdfs:///flink-sql-test-data/conf.json
sql文件位置     -sqlPath hdfs:///flink-sql-test-data/test_kafka_hdfs.sql
从保存点恢复    -fromSavepoint hdfs:///flink-checkpoints/test_join_pg_s/5395ccb928083875b088b1d640e69266/chk-3

配置conf.json(checkpoint部分)
{
	"intervalTime": 10000,
	"minPauseBetweenCheckpoints": 1000,
	"checkpointTimeout": 1000,
	"mode": "EXACTLY_ONCE",
	"enableExternalizedCheckpoint": true,
	"stateBackend": "filesystem",
	"stateCheckpointsDir": "hdfs:///flink-checkpoints/test_join_pg_s"
}

```
1.2通过http方式：   
```
step1. 在对应的客户机上启动http服务  ./flink-http.sh

step2. 通过post请求提交 
        url: http://{ip}:8083/ws/v1/jobs/new
        method: post
        Content-Type: application/json
        BODY : {
               	"s": "test-test",
               	"sqlPath": "hdfs:///flink-sql-test-data/test_kafka_hdfs/sql.sql",
               	"l": ["/bigdata/flink-yarn-1.5.1/opt/connectors",
               	"/bigdata/flink-yarn-1.5.1/opt/connectors/kafka08",
               	"/bigdata/flink-yarn-1.5.1/opt/connectors/kafka09",
               	"/bigdata/flink-yarn-1.5.1/opt/connectors/kafka010",
               	"/bigdata/flink-yarn-1.5.1/opt/connectors/kafka011"]
               }
               
参数定义：
会话名称        s test 
默认环境配置    d /bigdata/flink-1.5.1/conf/sql-client-defaults.yaml 
指定环境配置    e /bigdata/flink-1.5.1/conf/sql-client-defaults.yaml 
自定义jar lib   l /bigdata/flink-1.5.1/opt
自定义jar       j /bigdata/flink-1.5.1/opt/xxx.jar
配置文件位置    confPath hdfs:///flink-sql-test-data/conf.json
sql文件位置     sqlPath hdfs:///flink-sql-test-data/test_kafka_hdfs.sql
从保存点恢复    fromSavepoint hdfs:///flink-checkpoints/test_join_pg_s/5395ccb928083875b088b1d640e69266/chk-3

配置conf.json(checkpoint部分)
{
	"intervalTime": 10000,
	"minPauseBetweenCheckpoints": 1000,
	"checkpointTimeout": 1000,
	"mode": "EXACTLY_ONCE",
	"enableExternalizedCheckpoint": true,
	"stateBackend": "filesystem",
	"stateCheckpointsDir": "hdfs:///flink-checkpoints/test_join_pg_s"
}

```

2.list vertex
```
http get
http://gpu06:8088/proxy/{applicationId}/jobs/{jobId}/vertices/details
```

3.update vertex
```
http get
http://gpu06:8088/proxy/{:applicationId}/jobs/{:jobId}/update?vertexid={:vertexId}&parallelism={:parallelism}
```

