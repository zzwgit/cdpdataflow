### 使用tpc-h 数据 测试
1. [官网](http://www.tpc.org/information/benchmarks.asp?)下载数据tpc-h-tools.zip

2. 解压

3. 更改dbgen/makefile.suite成如下

   ```makefile
   #makefile.suite 的更改参数如下
   
   CC      = gcc
   # Current values for DATABASE are: INFORMIX, DB2, TDAT (Teradata)
   #                                  SQLSERVER, SYBASE, ORACLE, VECTORWISE
   # Current values for MACHINE are:  ATT, DOS, HP, IBM, ICL, MVS, 
   #                                  SGI, SUN, U2200, VMS, LINUX, WIN32 
   # Current values for WORKLOAD are:  TPCH
   
   DATABASE = POSTGRESQL     #程序给定参数没有postgresql ，修改tpcd.h 添加POSTGRESQL脚本
   MACHINE = LINUX
   WORKLOAD = TPCH
   ```

   

4. 修改dbgen/tpcd.h如下

   ```c
   //修改tpcd.h
   
   #ifdef POSTGRESQL
   #define GEN_QUERY_PLAN  "EXPLAIN"      
   #define START_TRAN      "BEGIN TRANSACTION"
   #define END_TRAN        "COMMIT;"
   #define SET_OUTPUT      ""
   #define SET_ROWCOUNT    "LIMIT %d\n"
   #define SET_DBASE       ""
   #endif /* VECTORWISE */
   ```

5. 修改dbgen/print.c

   ```c
   //#ifdef EOL_HANDLING
           if (sep)
   //#endif /* EOL_HANDLING */
           fprintf(target, "%c", SEPARATOR);
   
           return(0);
   }
   ```

   

6. cd dbgen && make -f makefile.suite

7. ```shell
   # 在dbgen目录下执行
   ./dbgen -s 1 -f   #-s 1 表示生成1G数据  -f覆盖之前产生的文件
   
   # 执行成功后会在dbgen目录下生成八个.tbl文件，可通过下列命令查看（在dbgen目录下）
   
   ls *.tbl
   
   # 看到产生八个tbl文件
   ```

8. dss.ddl 为见表语句， 

9. 见好表之后导入表格数据注意行首的 `\` 不能少

   ```plsql
   \Copy region FROM '/bigdata/tpch/dbgen/region.tbl' WITH DELIMITER AS '|';
   \Copy nation FROM '/bigdata/tpch/dbgen/nation.tbl' WITH DELIMITER AS '|';
   \Copy part FROM '/bigdata/tpch/dbgen/part.tbl' WITH DELIMITER AS '|';
   \Copy supplier FROM '/bigdata/tpch/dbgen/supplier.tbl' WITH DELIMITER AS '|';
   \Copy customer FROM '/bigdata/tpch/dbgen/customer.tbl' WITH DELIMITER AS '|';
   \Copy lineitem FROM '/bigdata/tpch/dbgen/lineitem.tbl' WITH DELIMITER AS '|';
   \Copy partsupp FROM '/bigdata/tpch/dbgen/partsupp.tbl' WITH DELIMITER AS '|';
   \Copy orders FROM '/bigdata/tpch/dbgen/orders.tbl' WITH DELIMITER AS '|';
   
   ```

   

10. 加约束条件,在 dss.ri 文件里

    ```sql
    -- For table REGION
    ALTER TABLE REGION
    ADD PRIMARY KEY (R_REGIONKEY);
    
    -- For table NATION
    ALTER TABLE NATION
    ADD PRIMARY KEY (N_NATIONKEY);
    
    ALTER TABLE NATION
    ADD FOREIGN KEY (N_REGIONKEY) references REGION;
    
    COMMIT WORK;
    
    -- For table PART
    ALTER TABLE PART
    ADD PRIMARY KEY (P_PARTKEY);
    
    COMMIT WORK;
    
    -- For table SUPPLIER
    ALTER TABLE SUPPLIER
    ADD PRIMARY KEY (S_SUPPKEY);
    
    ALTER TABLE SUPPLIER
    ADD FOREIGN KEY (S_NATIONKEY) references NATION;
    
    COMMIT WORK;
    
    -- For table PARTSUPP
    ALTER TABLE PARTSUPP
    ADD PRIMARY KEY (PS_PARTKEY,PS_SUPPKEY);
    
    COMMIT WORK;
    
    -- For table CUSTOMER
    ALTER TABLE CUSTOMER
    ADD PRIMARY KEY (C_CUSTKEY);
    
    ALTER TABLE CUSTOMER
    ADD FOREIGN KEY (C_NATIONKEY) references NATION;
    
    COMMIT WORK;
    
    -- For table LINEITEM
    ALTER TABLE LINEITEM
    ADD PRIMARY KEY (L_ORDERKEY,L_LINENUMBER);
    
    COMMIT WORK;
    
    -- For table ORDERS
    ALTER TABLE ORDERS
    ADD PRIMARY KEY (O_ORDERKEY);
    
    COMMIT WORK;
    
    -- For table PARTSUPP
    ALTER TABLE PARTSUPP
    ADD FOREIGN KEY (PS_SUPPKEY) references SUPPLIER;
    
    COMMIT WORK;
    
    ALTER TABLE PARTSUPP
    ADD FOREIGN KEY (PS_PARTKEY) references PART;
    
    COMMIT WORK;
    
    -- For table ORDERS
    ALTER TABLE ORDERS
    ADD FOREIGN KEY (O_CUSTKEY) references CUSTOMER;
    
    COMMIT WORK;
    
    -- For table LINEITEM
    ALTER TABLE LINEITEM
    ADD FOREIGN KEY (L_ORDERKEY)  references ORDERS;
    
    COMMIT WORK;
    
    ALTER TABLE LINEITEM
    ADD FOREIGN KEY (L_PARTKEY,L_SUPPKEY) references PARTSUPP;
    
    COMMIT WORK;
    ```

    

