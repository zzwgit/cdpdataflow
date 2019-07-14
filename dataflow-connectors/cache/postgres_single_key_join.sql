CREATE TABLE CUSTOMER
(
 C_CUSTKEY    int            NOT NULL,
 C_NAME       VARCHAR(25)    NOT NULL,
 C_ADDRESS    VARCHAR(40)    NOT NULL,
 C_NATIONKEY  int            NOT NULL,
 C_PHONE      VARCHAR(15)       NOT NULL,
 C_ACCTBAL    DECIMAL(15, 2) NOT NULL,
 C_MKTSEGMENT VARCHAR(10)       NOT NULL,
 C_COMMENT    VARCHAR(117)   NOT NULL,
 PRIMARY KEY (C_CUSTKEY),
 PERIOD FOR SYSTEM_TIME
 ) with (
	 type = 'postgres',
	 username = 'postgres',
	 password = '123456',
	 mode='async'|'sync',         ----
	 cache='LRU'|'ALL',          ----
 	cacheTTLms='10000',    ----
 	cacheSize='60000',     ----
	 tablename = 'customer',
	 dburl = 'jdbc:postgresql://172.19.0.108:5432/db_dmx_stage'
);

CREATE TABLE ORDERS
(
 O_ORDERKEY      int            NOT NULL,
 O_CUSTKEY       int            NOT NULL,
 O_ORDERSTATUS   VARCHAR(1)        NOT NULL,
 O_TOTALPRICE    DECIMAL(15, 2) NOT NULL,
 O_ORDERDATE     DATE           NOT NULL,
 O_ORDERPRIORITY VARCHAR(15)       NOT NULL,
 O_CLERK         VARCHAR(15)       NOT NULL,
 O_SHIPPRIORITY  int            NOT NULL,
 O_COMMENT       VARCHAR(79)    NOT NULL,
 PRIMARY KEY (O_CUSTKEY)
 ) with (
	 type = 'csv',
	 firstLineAsHeader='false',
	 fielddelim='|',
	 path = 'hdfs://172.19.0.16:9000/flink-sql-test-data/orders.tbl'
	);

 create table t1
 (
  C_NAME varchar,
  O_COMMENT        varchar
 ) with (
	 type = 'csv',
	 updateMode='append',
	 path = 'hdfs:///flink-sql-test-data/tphc_join.txt'
	);

 insert into t1
 SELECT c.C_NAME,
 o.O_COMMENT
 from  ORDERS as o join
 CUSTOMER FOR SYSTEM_TIME AS OF PROCTIME() as c
 ON o.O_CUSTKEY = c.C_CUSTKEY;

 commit  tpch_customer_orders_single_join;
