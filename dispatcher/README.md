# dispatcher

参数定义：
```
-s：会话名称  
-d：默认环境配置  
-e：指定环境配置  
-j：自定义jar   
-l：自定义jar libs  
-sqlPath：sql文件位置  
-intervalTime：checkpoint时间间隔，单位ms（1000）  
-mode：checkpoint模式（EXACTLY_ONCE or AT_LEAST_ONCE）  
-stateBackend：checkpoint文件存储方式（filesystem or rocksdb）  
-stateCheckpointsDir：checkpoint文件存储路径（hdfs:///flink/checkpoints）  
-fromSavepoint：从指定保存点恢复(hdfs://namenode:40010/flink/checkpoints/xxx)  
```


启动命令：
```
export flink conf环境变量
export FLINK_CONF_DIR=/bigdata/flink-1.5.1/conf &&

java classpatch设置 
java -cp 
/bigdata/flink-1.5.1/lib/log4j-1.2.17.jar:
/bigdata/flink-1.5.1/lib/slf4j-log4j12-1.7.7.jar:
/bigdata/flink-1.5.1/lib/flink-dist_2.11-1.5.1.jar:
/bigdata/flink-1.5.1/lib/flink-shaded-hadoop2-uber-1.5.1.jar:
/bigdata/flink-1.5.1/lib/flink-python_2.11-1.5.1.jar:
/bigdata/flink-1.5.1/opt/sql-client/flink-sql-client-1.5.1.jar:
/opt/apps/zf/blink-sql/dispatcher-1.0.jar io.infinivision.flink.core.Dispatcher

相关参数 
-s test 
-d /bigdata/flink-1.5.1/conf/sql-client-defaults.yaml 
-l /bigdata/flink-1.5.1/opt 
-l /bigdata/flink-1.5.1/opt/connectors 
-l /bigdata/flink-1.5.1/opt/connectors/kafka011 
-sqlPath /opt/apps/zf/blink-sql/test_join_pg_s.sql 

配置checkpoint
-intervalTime 10000 
-mode EXACTLY_ONCE 
-stateBackend filesystem 
-stateCheckpointsDir hdfs:///flink-checkpoints/test_join_pg_s 

配置从保存点恢复
-fromSavepoint hdfs:///flink-checkpoints/test_join_pg_s/5395ccb928083875b088b1d640e69266/chk-3

例如：
export FLINK_CONF_DIR=/bigdata/flink-1.5.1/conf && java -cp /bigdata/flink-1.5.1/lib/log4j-1.2.17.jar:/bigdata/flink-1.5.1/lib/slf4j-log4j12-1.7.7.jar:/bigdata/flink-1.5.1/lib/flink-dist_2.11-1.5.1.jar:/bigdata/flink-1.5.1/lib/flink-shaded-hadoop2-uber-1.5.1.jar:/bigdata/flink-1.5.1/lib/flink-python_2.11-1.5.1.jar:/bigdata/flink-1.5.1/opt/sql-client/flink-sql-client-1.5.1.jar:/opt/apps/zf/blink-sql/dispatcher-1.0.jar io.infinivision.flink.core.Dispatcher -s test -d /bigdata/flink-1.5.1/conf/sql-client-defaults.yaml -l /bigdata/flink-1.5.1/opt -l /bigdata/flink-1.5.1/opt/connectors -l /bigdata/flink-1.5.1/opt/connectors/kafka011 -sqlPath /opt/apps/zf/blink-sql/test_join_pg_s.sql 
```