package io.infinivision.flink.examples;

import com.google.common.collect.Maps;
import io.infinivision.flink.connectors.clickhouse.ClickHouseTableFactory;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.RichTableSchema;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.TableEnvironment;
import org.apache.flink.table.api.java.StreamTableEnvironment;
import org.apache.flink.table.api.types.DataTypes;
import org.apache.flink.table.api.types.InternalType;
import org.apache.flink.table.dataformat.BaseRow;
import org.apache.flink.table.sources.BatchTableSource;
import org.apache.flink.table.sources.StreamTableSource;
import org.apache.flink.table.util.TableProperties;
import org.apache.flink.types.Row;

import java.util.Map;

public class ClickHouseSourceTest {

    public static void main(String[] args) throws Exception {

        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);
        StreamTableEnvironment tEnv = TableEnvironment.getTableEnvironment(env);

        Map<String, String> map = Maps.newHashMap();
        map.put("updatemode", "append");
        map.put("version", "19.9.1");
//        map.put("username", "1");
//        map.put("password", "1");
        map.put("tablename", "test_zf2");
        map.put("dburl", "jdbc:clickhouse://172.19.0.13:8123/default");


        TableProperties tableProperties = new TableProperties();
        tableProperties.putProperties(map);


        String[] columnNames = new String[]{"id", "name"};
        InternalType[] columnTypes = new InternalType[]{DataTypes.LONG, DataTypes.STRING};
        RichTableSchema richSchema = new RichTableSchema(columnNames, columnTypes);
        richSchema.setPrimaryKey("id");
        tableProperties.putSchemaIntoProperties(richSchema);

        ClickHouseTableFactory tableFactory = new ClickHouseTableFactory();

//        StreamTableSource<BaseRow> source = tableFactory.createStreamTableSource(tableProperties.toMap());
//        tEnv.registerTableSource("test_zf", source);
//        String sql = "SELECT id,name FROM test_zf";
//        Table out = tEnv.sqlQuery(sql);
//        DataStream<Row> dsRow =tEnv.toAppendStream(out, Row.class);
//        dsRow.print();

        BatchTableSource<BaseRow> batchSource = tableFactory.createBatchTableSource(tableProperties.toMap());
        tEnv.registerTableSource("test_zf", batchSource);
        String sql = "SELECT id,name FROM test_zf";
        Table out = tEnv.sqlQuery(sql);
        out.print();
//        DataStream<Row> dsRow =tEnv.toAppendStream(out, Row.class);
//        dsRow.print();

        env.execute("Postgres Table Source Exercise");


    }
}
