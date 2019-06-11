package io.infinivision.flink.examples;

import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import io.infinivision.flink.connectors.clickhouse.ClickHouseTableFactory;
import org.apache.commons.collections.MapUtils;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.RichTableSchema;
import org.apache.flink.table.api.TableEnvironment;
import org.apache.flink.table.api.types.DataTypes;
import org.apache.flink.table.api.types.InternalType;
import org.apache.flink.table.sinks.TableSink;
import org.apache.flink.table.sources.TableSource;
import org.apache.flink.table.sources.csv.CsvTableSource;
import org.apache.flink.table.util.TableProperties;

import java.util.List;
import java.util.Map;

public class ClickHouseTest {

    public static void main(String[] args) throws Exception {

        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);
        TableEnvironment tEnv = TableEnvironment.getTableEnvironment(env);
//        TableEnvironment tEnv = TableEnvironment.getBatchTableEnvironment(env);
        TableSource t_source = CsvTableSource.builder()
                .path("D:\\test-files\\in\\train10.csv")
                .field("aid", DataTypes.STRING)
                .field("uid", DataTypes.STRING)
                .field("label", DataTypes.STRING)
                .enableEmptyColumnAsNull()
                .ignoreFirstLine()
                .build();
        tEnv.registerTableSource("t_source", t_source);


        Map<String, String> map = Maps.newHashMap();
        map.put("updatemode", "append");
        map.put("version", "19.9.1");
//        map.put("username", "1");
//        map.put("password", "1");
        map.put("tablename", "test_zf");
        map.put("dburl", "jdbc:clickhouse://172.19.0.13:8123/default");
        TableProperties postgresSinkTableProperties = new TableProperties();
        postgresSinkTableProperties.putProperties(map);

        String[] columnNames = new String[]{"id", "name"};
        InternalType[] columnTypes = new InternalType[]{DataTypes.INT, DataTypes.STRING};

        RichTableSchema richSchema = new RichTableSchema(columnNames, columnTypes);
        List<String> uniqueKey1 = Lists.newArrayList("id");
        List<List<String>> uniqueKeys = Lists.newArrayList();
        uniqueKeys.add(uniqueKey1);
        richSchema.setUniqueKeys(uniqueKeys);
        postgresSinkTableProperties.putSchemaIntoProperties(richSchema);

        ClickHouseTableFactory clickHouseTableFactory = new ClickHouseTableFactory();
        TableSink t_output = clickHouseTableFactory.createBatchCompatibleTableSink(postgresSinkTableProperties.toMap());

        tEnv.registerTableSink("t_output", t_output);

        String sql = "insert into t_output select cast(aid as INT) as id ,label as name from t_source";

        tEnv.sqlUpdate(sql);

        env.execute();

    }
}
