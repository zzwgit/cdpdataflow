# dispatcher

1.任务提交：
```
./flink-sql.sh run -s test -confPath hdfs:///flink-sql-test-data/conf.json -sqlPath hdfs:///flink-sql-test-data/test_kafka_hdfs.sql

参数定义：
会话名称        -s test 
默认环境配置    -d /bigdata/flink-1.5.1/conf/sql-client-defaults.yaml 
指定环境配置    -e /bigdata/flink-1.5.1/conf/sql-client-defaults.yaml 
自定义jar lib   -l /bigdata/flink-1.5.1/opt
自定义jar       -j /bigdata/flink-1.5.1/lib/flink-dist_2.11-1.5.1.jar
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

2.list vertex
```
http://gpu06:8088/proxy/{applicationId}/jobs/{jobId}/vertices/details
```

3.update vertex
```
http://gpu06:8088/proxy/{:applicationId}/jobs/{:jobId}/update?vertexid={:vertexId}&parallelism={:parallelism}
```

