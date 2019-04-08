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

* table source ddl synx
```sql
CREATE TABLE tableName(
  columnName dataType,
  columnName dataType,
  ...
) WITH (
  propertyName=propertyValue,
  propertyName=propertyValue,
  ...
)
```

### Properties description
| type  | connector.version  | username  | password  | tablename  | dburl  | cache | cacheTTLms | mode |
| :--:  | :---------------:  | :------:  | :------:  | :-------:  | :---:  | :---: | :--------: | :--: |
| postgres  | 9.4  | 用户名  | 密码  | 表名  | 数据库连接URL | NONE,LRU,ALL | LRU cache TTL | async,sync |

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