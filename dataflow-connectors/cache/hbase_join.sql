create table hbase121_source (
  user_id VARCHAR NOT NULL,
  item_id VARCHAR NOT NULL,
  category_id VARCHAR NOT NULL,
  behavior VARCHAR NOT NULL,
  timeline BIGINT NOT NULL,
PRIMARY KEY(user_id)
) with (
 type = 'csv',
  firstLineAsHeader='false',
  path = 'hdfs:///user/hongtaozhang/test_data/UserBehavior1000W.csv'
);

create table hbase121_dimension (
  rowKey VARCHAR,
  `result_f1.behavior` VARCHAR,
  `result_f1.user_id` VARCHAR,
  `result_f1.user_name` VARCHAR,
  `result_f1.user_type` VARCHAR,
  PRIMARY KEY(rowKey),
 PERIOD FOR SYSTEM_TIME
) with (
    `type`='HBASE',
    `tableName`='infinivision:test_sink',
    `version`='1.2.1',
	 mode='async',
         cache='LRU'|'ALL',
        cacheTTLms='10000',-- ALL模式下不需要
        cacheSize='60000'
);
create table scv_sink (
  user_id VARCHAR NOT NULL,
  item_id VARCHAR NOT NULL
) with (
 type = 'csv',
updateMode='append',
  path = 'hdfs:///user/hongtaozhang/test_data/UserBehavior1000W_join.csv'
);

INSERT INTO scv_sink
SELECT
source.user_id ,
sc.`result_f1.user_name`
FROM hbase121_source as source
join hbase121_dimension  FOR SYSTEM_TIME AS OF PROCTIME() as sc
on source.user_id = sc.rowKey;


commit hbase_join;