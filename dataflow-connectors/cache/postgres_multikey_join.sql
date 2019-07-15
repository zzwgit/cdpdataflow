CREATE TABLE PARTSUPP ( PS_PARTKEY     int NOT NULL,
                             PS_SUPPKEY     int NOT NULL,
                             PS_AVAILQTY    int NOT NULL,
                             PS_SUPPLYCOST  DECIMAL(15,2)  NOT NULL,
                             PS_COMMENT     VARCHAR(199) NOT NULL ,
 INDEX (PS_PARTKEY, PS_SUPPKEY),
 PERIOD FOR SYSTEM_TIME
 ) with (
	 type = 'postgres',
	 username = 'postgres',
	 password = '123456',
	 mode='async'|'sync',
	 cache='LRU'|'ALL',
 	cacheTTLms='10000000',
 	cacheSize='10000', -- ALL模式下不需要
	 tablename = 'partsupp',
	 dburl = 'jdbc:postgresql://172.19.0.108:5432/db_dmx_stage'
);

CREATE TABLE LINEITEM ( L_ORDERKEY    int NOT NULL,
                             L_PARTKEY     int NOT NULL,
                             L_SUPPKEY     int NOT NULL,
                             L_LINENUMBER  int NOT NULL,
                             L_QUANTITY    DECIMAL NOT NULL,
                             L_EXTENDEDPRICE  DECIMAL(15,2) NOT NULL,
                             L_DISCOUNT    DECIMAL(15,2) NOT NULL,
                             L_TAX         DECIMAL(15,2) NOT NULL,
                             L_RETURNFLAG  VARCHAR(1) NOT NULL,
                             L_LINESTATUS  VARCHAR(1) NOT NULL,
                             L_SHIPDATE    varchar NOT NULL,
                             L_COMMITDATE  varchar NOT NULL,
                             L_RECEIPTDATE varchar NOT NULL,
                             L_SHIPINSTRUCT VARCHAR(25) NOT NULL,
                             L_SHIPMODE     VARCHAR(10) NOT NULL,
                             L_COMMENT      VARCHAR(44) NOT NULL,
 primary key  (L_PARTKEY, L_SUPPKEY)
 ) with (
	 type = 'csv',
	 firstLineAsHeader='false',
	 fielddelim='|',
	 path = 'hdfs://172.19.0.16:9000/flink-sql-test-data/lineitem.tbl'
	);

 create table t1
 (
  PS_AVAILQTY int,
  L_LINENUMBER  int
 ) with (
	 type = 'csv',
	 updateMode='append',
	 path = 'hdfs:///flink-sql-test-data/tphc_join_big2'
	);

 insert into t1
 SELECT c.PS_PARTKEY,
 l.L_SUPPKEY
 from  LINEITEM  as l join
 PARTSUPP FOR SYSTEM_TIME AS OF PROCTIME() as c
 ON l.L_PARTKEY=c.PS_PARTKEY and l.L_SUPPKEY=c.PS_SUPPKEY;

 commit  tpch_customer_orders_single_join;
