# RuleEngine
tagging system based on Apache Flink

## 编译打包
* 推荐使用maven 3.2.5版本
* mvn clean install

## unit test
目前unit test running 依赖于本地的postgres server. 所以在跑之前在本地安装postgres，可以通过
docker的方式安装
```bash
docker pull postgres:9.4
docker run --name postgres -e POSTGRES_PASSWORD=123456 -p 25432:5432 -d postgres:9.4
```


## Postgres Connector

### table DDL
```sql
CREATE TABLE tableName(
  columnName dataType,
  columnName dataType,
  ...
) WITH (
  propertyName=propertyValue,
  propertyName=propertyValue,
  PRIMARY KEY(columnName)
  [UNIQUE] INDEX(columnName,...)
  ...
)
```


### parameter description for dimension table and sink table
| 名字  |   描述  | 必选  | 默认值  |
| :--:  | :-----------:  | :------:  |    :------:    |
| type  |   postgres (both)    |  true    |      no        |
| version  |   (lower)9.4/9.5(upper)(both)     |  true    |      no        |
| username  | DB username (both) |  true    |      no        |
| tablename  | DB tablename (both) |  true    |      no        |
| password  | db password (both)    |  true    |      no        |
| dburl  | db connection url     |  true    |      no        |
| updatemode  | append/upsert(for sink table) |  false    |   upsert    |
| cache  | none/LRU/ALL(for dimension table) |  false    |     none      |
| cacheTTLms  | cache TTL(for dimension table) |  false    |    3600000     |
| mode  | async/sync(for dimension table) |  false    |    async        |


### Note:

* 维表至少需要一个index在on条件中
* 作为sink table时，在upsert模式下必须包含一个primary key或者unique index，append模式则不要求 所有记录append only


### Temporal Table Join Example
```sql
-- create dimision table
CREATE TABLE csv_adfeature (
  aid VARCHAR NOT NULL,
  advertiser_id VARCHAR NOT NULL,
  campaign_id VARCHAR NOT NULL,
  creative_id VARCHAR NOT NULL,
  creative_size VARCHAR NOT NULL,
  category_id VARCHAR NOT NULL,
  product_id VARCHAR NOT NULL,
  product_type VARCHAR NOT NULL,
  PRIMARY KEY(aid)
) WITH (
  type = 'postgres',
  connector.version = '9.4',
  username = 'postgres',
  password = '123456',
  tablename = 'adFeature',
  dburl = 'jdbc:postgresql://localhost:5432/postgres'
);

-- create probe side table
create table train (
  aid VARCHAR NOT NULL,
  uid VARCHAR NOT NULL,
  label VARCHAR NOT NULL
) with (
  type = 'csv',
  firstLineAsHeader='true',
  path = 'file:///bigdata/datasets/train10.csv'
);

-- temporal table join sql query
SELECT
p.aid, p.uid, b.advertiser_id
FROM train AS p
INNER JOIN
adfeature FOR SYSTEM_TIME AS OF PROCTIME() AS b
ON p.aid = b.aid;

```

### Table Sink Example(append mode)
```sql
-- create source table
CREATE TABLE input (
  aid VARCHAR NOT NULL,
  advertiser_id VARCHAR NOT NULL,
  campaign_id VARCHAR NOT NULL,
  creative_id VARCHAR NOT NULL,
  creative_size VARCHAR NOT NULL,
  category_id VARCHAR NOT NULL,
  product_id VARCHAR NOT NULL,
  product_type VARCHAR NOT NULL,
) WITH (
  type = 'postgres',
  version = '9.4',
  username = 'postgres',
  password = '123456',
  tablename = 'adFeature',
  dburl = 'jdbc:postgresql://localhost:5432/postgres'
);

-- create sink table
create table output (
  aid VARCHAR NOT NULL,
  uid VARCHAR NOT NULL,
  label VARCHAR NOT NULL
) with (
  type = 'postgres',
  version = '9.4',
  username = 'postgres',
  password = '123456',
  tablename = 'append_output',
  dburl = 'jdbc:postgresql://localhost:5432/postgres',
  updateMode = 'append'
);

INSERT INTO output
SELECT aid, uid, label
FROM input
```

### Table Sink Example(upsert mode)
```sql
-- create source table
CREATE TABLE input (
  aid VARCHAR NOT NULL,
  advertiser_id VARCHAR NOT NULL,
  campaign_id VARCHAR NOT NULL,
  creative_id VARCHAR NOT NULL,
  creative_size VARCHAR NOT NULL,
  category_id VARCHAR NOT NULL,
  product_id VARCHAR NOT NULL,
  product_type VARCHAR NOT NULL,
) WITH (
  type = 'postgres',
  version = '9.5',
  username = 'postgres',
  password = '123456',
  tablename = 'adFeature',
  dburl = 'jdbc:postgresql://localhost:5432/postgres'
);

-- create sink table
create table output (
  aid VARCHAR NOT NULL,
  count BIGINT NOT NULL
) with (
  type = 'postgres',
  version = '9.5',
  username = 'postgres',
  password = '123456',
  tablename = 'append_output',
  dburl = 'jdbc:postgresql://localhost:5432/postgres',
  updateMode = 'upsert',
  PRIMARY KEY('aid') -- 或者 UNIQUE INDEX('aid') 必填
);

INSERT INTO output
SELECT aid, count(uid)
FROM input
GROUP BY aid

```