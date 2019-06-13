Flink HBase Connector SQL Operations
======================================

Postgres Connector可以作为Table Source/Table Sink/Temporal Table(async mode & sync mode)

### table DDL
```sql
CREATE TABLE tableName(
  columnName dataType,
  columnName dataType,
  ...
  PRIMARY KEY(columnName),
  [UNIQUE] INDEX(columnName,...)
) WITH (
  type='hbase'
  propertyName=propertyValue,
  propertyName=propertyValue,
  ...
)
```

### 参数描述(source/sink/temporal)
| 名字  |   描述  | 必选  | 默认值  |
| :--:  | :-----------:  | :------:  |    :------:    |
| type  |   postgres (both)    |  true    |      no        |
| version  |   1.2.1     |  true    |      no        |
| tablename  | DB tablename (both) |  true    |      no        |
| batchSize  | 写入批大小 |  false    |      5000        |
| updatemode  | upsert |  false    |   upsert    |
| cache  | none/LRU/ALL(only for dimension table) |  false    |     none      |
| cacheTTLms  | cache TTL(only for dimension table) |  false    |    3600000     |
| mode  | async/sync(only for dimension table) |  false    |    async        |
| hbase.security.auth.enable  | auth enable or not |  false    |    false        |
| hbase.security.auth.authentication  | 认证方式 |  false    |    kerberos        |
| keytabPath  | keytab 文件路径 |  false    |    none        |
| principal  | kerberos principal |  false    |    none        |


### Note:

* 维表至少需要一个index在on条件中
* 作为sink table时，在upsert模式下必须包含一个primary key或者unique index，append模式则不要求 所有记录append only

### Table Source Example
```sql
create table hbase121_source (
  rowKey VARCHAR,
  `train.label` INT,
  PRIMARY KEY(rowKey)
) with (
    `type`='HBASE',
    `tableName`='infinivision:train_sink',
    `version`='1.2.1',
    `batchSize`='1000'
);

create table train_csv_sink (
  uuid VARCHAR NOT NULL,
  label INT NOT NULL
) with (
  type = 'csv',
  path = 'hdfs:///user/infinivision_flink_user/train_csv_sink.csv'
);


INSERT INTO train_csv_sink
SELECT * FROM
hbase121_source as s;
```

### Table Sink Example
```sql
-- source table
create table train (
  aid INT NOT NULL,
  uid INT NOT NULL,
  label INT NOT NULL
) with (
  type = 'csv',
  firstLineAsHeader='true',
  path = 'hdfs:///user/infinivision_flink_user/train.csv'
);

-- create hbase 1.2.1 (with batch put)
create table hbase121_sink (
  rowKey VARCHAR,
  `train.label` INT,
  PRIMARY KEY(rowKey)
) with (
    `type`='HBASE',
    `tableName`='infinivision:train_sink',
    `version`='1.2.1',
    `batchSize`='5000'
);

INSERT INTO hbase121_sink
SELECT 
  CONCAT_WS('-', cast(aid AS VARCHAR), cast(uid AS VARCHAR)) as rowKey,
  label
FROM train;

```

### Temporal Table Join Example
```sql
-- create probe table
create table train (
  aid INT NOT NULL,
  uid INT NOT NULL,
  label INT NOT NULL
) with (
  type = 'csv',
  firstLineAsHeader='true',
  path = 'hdfs:///user/infinivision_flink_user/train.csv'
);

create table hbase_adfeature (
  aid INT,
  `feature.advertiser_id` INT,
  `feature.campaign_id` INT,
  `feature.creative_id` INT,
  `feature.creative_size` INT,
  `feature.category_id` INT,
  `feature.product_id` INT,
  `feature.product_type` INT,
  PRIMARY KEY(aid)
) with (
    `type`='HBASE',
    `tableName`='infinivision:ad_feature',
    `version`='1.2.1',
    `mode`='async',
    `hbase.security.auth.enable`='true',
    `hbase.security.authentication`='kerberos',
    `keyTabPath`='hdfs:///user/infinivision_flink_user/infinivision_flink_user.keytab',
    `principal`='infinivision_flink_user'
);

-- create hbase result table
create table hbase_async_sink (
  rowKey VARCHAR NOT NULL,
  `result.advertiser_id` INT NOT NULL,
  `result.category_id` INT NOT NULL,
  PRIMARY KEY(rowKey)
) with (
    `type`='HBASE',
    `tableName`='infinivision:ad_async_result',
    `version`='1.2.1',
    `batchSize` = '1000'
);

-- temporal Table Join DML
INSERT INTO hbase_async_sink
SELECT 
	CONCAT_WS('-', cast(probe.aid AS VARCHAR), cast(probe.uid AS VARCHAR)) as rowKey,
	side.`feature.advertiser_id`, side.`feature.category_id`
FROM train as probe
INNER JOIN hbase_adfeature FOR SYSTEM_TIME AS OF PROCTIME() AS side
ON probe.aid = side.aid;

```