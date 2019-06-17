Flink ClickHouse Connector SQL Operations
======================================

ClickHouse Connector可以作为Table Source/Table Sink

### table DDL
```sql
CREATE TABLE tableName(
  columnName dataType,
  columnName dataType,
  ...
  PRIMARY KEY(columnName),
  [UNIQUE] INDEX(columnName,...)
) WITH (
  type = 'clickhouse',
  version = '19.9.1',
  username = 'gpadmin',
  password = 'Welcome123@',
  tablename = 'test_zf2',
  fetchsize = '100',
  dburl = 'jdbc:clickhouse://172.19.0.13:8123/default'
)
```

### 参数描述(source/sink)
| 名字  |   描述  | 必选  | 默认值  |
| :--:  | :-----------:  | :------:  |    :------:    |
| type  |   clickhouse (both)    |  true    |      no        |
| version  |   19.9.1(both)      |  true    |      no        |
| username  |   用户名(both)      |  true    |      no        |
| password  |   密码(both)      |  true    |      no        |
| dburl  |   数据库链接(both)      |  true    |      no        |
| tablename  | 表名(both)  |  true    |              |
| fetchsize  | 批量读取数据条数(source)  |  false    |      1000        |
| updatemode  | append(sink)  |  false    |   append    |


### Note:
* 作为sink table时，仅支持append模式

### Table Source/Sink Example
```sql
/**source**/
CREATE TABLE source_clickhouse (
	id bigint NOT NULL,
	name VARCHAR NOT NULL,
	PRIMARY KEY(id)
) WITH (
	type = 'clickhouse',
	version = '19.9.1',
	tablename = 'test_zf2',
	fetchsize = '100',
	dburl = 'jdbc:clickhouse://172.19.0.13:8123/default'
);

/**sink**/
create table sink_clickhouse (
  create_datetime VARCHAR,
  id bigint NOT NULL,
  name VARCHAR NOT NULL,
  PRIMARY KEY(id)
) with (
  type = 'clickhouse',
  version = '19.9.1',
  tablename = 'test_zf3',
  dburl = 'jdbc:clickhouse://172.19.0.13:8123/default',
  updateMode = 'append'
);

insert into sink_clickhouse 
select 
DATE_FORMAT(CURRENT_TIMESTAMP, 'yyyy-MM-dd HH:mm:ss') as create_datetime,
id ,
name 
from source_clickhouse;
```