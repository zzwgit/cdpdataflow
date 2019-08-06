package io.infinivision.flink.connectors.clickhouse;

import io.infinivision.flink.connectors.jdbc.BaseRowJDBCInputFormat;
import io.infinivision.flink.connectors.utils.CommonTableOptions;
import org.apache.flink.api.java.io.jdbc.JDBCOptions;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.TableSchema;
import org.apache.flink.table.api.types.DataType;
import org.apache.flink.table.api.types.InternalType;
import org.apache.flink.table.api.types.RowType;
import org.apache.flink.table.api.types.TypeConverters;
import org.apache.flink.table.dataformat.BaseRow;
import org.apache.flink.table.plan.stats.TableStats;
import org.apache.flink.table.sources.BatchTableSource;
import org.apache.flink.table.sources.StreamTableSource;
import org.apache.flink.table.typeutils.BaseRowTypeInfo;
import org.apache.flink.table.util.TableProperties;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class ClickHouseTableSource implements
        StreamTableSource<BaseRow>,
        BatchTableSource<BaseRow>{

    private static final Logger LOG = LoggerFactory.getLogger(ClickHouseTableSource.class);

    public static final String DRIVERNAME = "ru.yandex.clickhouse.ClickHouseDriver";

    private TableProperties tableProperties;
    private String[] columnNames;
    private InternalType[] columnTypes;
    private Boolean[] columnNullables;

    private String[] primaryKeys;
    private String[][] uniqueKeys;
    private String[][] normalIndexes;

    private RowType returnType;
    private BaseRowTypeInfo returnTypeInfo;

    public ClickHouseTableSource(TableProperties tableProperties, String[] columnNames, InternalType[] columnTypes, Boolean[] columnNullables, String[] primaryKeys, String[][] uniqueKeys, String[][] normalIndexes) {
        this.tableProperties = tableProperties;
        this.columnNames = columnNames;
        this.columnTypes = columnTypes;
        this.columnNullables = columnNullables;
        this.primaryKeys = primaryKeys;
        this.uniqueKeys = uniqueKeys;
        this.normalIndexes = normalIndexes;

        this.returnType = new RowType(columnTypes, columnNames);
        this.returnTypeInfo = TypeConverters.toBaseRowTypeInfo(returnType);
    }

    private BaseRowJDBCInputFormat createInputFormat(String queryTemplate) {
        // build the JDBCInputFormat
        String userName = tableProperties.getString(JDBCOptions.USER_NAME);
        String password = tableProperties.getString(JDBCOptions.PASSWORD);
        String dbURL = tableProperties.getString(JDBCOptions.DB_URL);
        Integer fetchSize = Integer.parseInt(tableProperties.getString(CommonTableOptions.FETCH_SIZE));

        return BaseRowJDBCInputFormat.buildBaseRowJDBCInputFormat()
                .setUsername(userName)
                .setPassword(password)
                .setDrivername(DRIVERNAME)
                .setDBUrl(dbURL)
                .setQuery(queryTemplate)
                .setRowTypeInfo(returnTypeInfo)
                .setFetchSize(fetchSize)
                .finish();
    }

    @Override
    public DataStream<BaseRow> getBoundedStream(StreamExecutionEnvironment streamEnv) {
        //return getDataStream(streamEnv);
        // build query template
        StringBuilder fields = new StringBuilder();
        for (int i = 0; i < columnNames.length; i++) {
            if (i != 0) {
                fields.append(", ");
            }
            fields.append(columnNames[i]);
        }

        String tableName = tableProperties.getString(JDBCOptions.TABLE_NAME);

        String queryTemplate = String.format("SELECT %s FROM %s", fields, tableName);
        return streamEnv.createInput(createInputFormat(queryTemplate), returnTypeInfo);
    }

    @Override
    public DataStream<BaseRow> getDataStream(StreamExecutionEnvironment execEnv) {
        // build query template
        StringBuilder fields = new StringBuilder();
        for (int i = 0; i < columnNames.length; i++) {
            if (i != 0) {
                fields.append(", ");
            }
            fields.append(columnNames[i]);
        }

        String tableName = tableProperties.getString(JDBCOptions.TABLE_NAME);

        String queryTemplate = String.format("SELECT %s FROM %s", fields, tableName);
        return execEnv.createInput(createInputFormat(queryTemplate), returnTypeInfo);
    }

    @Override
    public DataType getReturnType() {
        return returnType;
    }

    @Override
    public TableSchema getTableSchema() {
        TableSchema.Builder builder = TableSchema.builder();

        for(int index = 0; index < columnNames.length; index++) {
            builder.column(columnNames[index], columnTypes[index], columnNullables[index]);
        }

        if(primaryKeys.length > 0) {
            builder.primaryKey(primaryKeys);
        }

        for (String[] uk : uniqueKeys) {
            builder.uniqueIndex(uk);
        }

        for (String[] nk : normalIndexes) {
            builder.normalIndex(nk);
        }
        return builder.build();
    }

    @Override
    public String explainSource() {
        return "ClickHouse";
    }

    @Override
    public TableStats getTableStats() {
        return null;
    }

    public static ClickHouseTableSourceBuilder builder() {
        return new ClickHouseTableSourceBuilder();
    }
}
