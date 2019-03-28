package io.infinivision.flink.connectors.postgres;

import io.infinivision.flink.connectors.jdbc.BaseRowJDBCInputFormat;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.TableSchema;
import org.apache.flink.table.api.functions.AsyncTableFunction;
import org.apache.flink.table.api.functions.TableFunction;
import org.apache.flink.table.api.types.DataType;
import org.apache.flink.table.api.types.InternalType;
import org.apache.flink.table.api.types.RowType;
import org.apache.flink.table.api.types.TypeConverters;
import org.apache.flink.table.calcite.FlinkTypeFactory;
import org.apache.flink.table.dataformat.BaseRow;
import org.apache.flink.table.plan.stats.TableStats;
import org.apache.flink.table.sources.*;
import org.apache.flink.table.typeutils.BaseRowTypeInfo;
import org.apache.flink.table.util.TableProperties;
import org.apache.flink.util.Preconditions;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.HashSet;
import java.util.LinkedHashMap;
import java.util.Set;
import java.util.stream.Collectors;


public class PostgresTableSource implements
        StreamTableSource<BaseRow>,
        BatchTableSource<BaseRow>,
        LookupableTableSource<BaseRow> {
    private static final Logger LOG = LoggerFactory.getLogger(PostgresTableSource.class);
    public static final String DRIVERNAME = "org.postgresql.Driver";

    private TableProperties tableProperties;
    private String[] columnNames;
    private InternalType[] columnTypes;
    private Boolean[] columnNullables;

    private String[] primaryKeys;
    private String[][] uniqueKeys;
    private String[][] normalIndexes;

    private RowType returnType;
    private BaseRowTypeInfo returnTypeInfo;

    public PostgresTableSource(
            TableProperties tableProperties,
            String[] columnNames,
            InternalType[] columnTypes,
            Boolean[] columnNullables,
            String[] primaryKeys,
            String[][] uniqueKeys,
            String[][] normalIndexes) {
        this.tableProperties = tableProperties;
        this.columnNames = columnNames;
        this.columnTypes = columnTypes;
        this.columnNullables = columnNullables;
        this.primaryKeys = primaryKeys;
        this.uniqueKeys = uniqueKeys;
        this.normalIndexes = normalIndexes;

        // return type
        this.returnType = new RowType(columnTypes, columnNames);
        this.returnTypeInfo = TypeConverters.toBaseRowTypeInfo(returnType);
    }


    private BaseRowJDBCInputFormat createInputFormat(String queryTemplate) {
        // build the JDBCInputFormat
        String userName = tableProperties.getString(PostgresOptions.USER_NAME);
        String password = tableProperties.getString(PostgresOptions.PASSWORD);
        String dbURL = tableProperties.getString(PostgresOptions.DB_URL);

        return BaseRowJDBCInputFormat.buildBaseRowJDBCInputFormat()
                .setUsername(userName)
                .setPassword(password)
                .setDrivername(DRIVERNAME)
                .setDBUrl(dbURL)
                .setQuery(queryTemplate)
                .setRowTypeInfo(returnTypeInfo)
                .finish();
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

        String tableName = tableProperties.getString(PostgresOptions.TABLE_NAME);

        String queryTemplate = String.format("SELECT %s FROM %s", fields, tableName);
        return execEnv.createInput(createInputFormat(queryTemplate), returnTypeInfo);
    }

    @Override
    public TableSchema getTableSchema() {
        TableSchema.Builder builder = TableSchema.builder();

        for(int index = 0; index < columnNames.length; index++) {
            builder.column(columnNames[index], columnTypes[index], columnNullables[index]);
        }

        builder.primaryKey(primaryKeys);

        for (String[] uk : uniqueKeys) {
            builder.uniqueIndex(uk);
        }

        for (String[] nk : normalIndexes) {
            builder.normalIndex(nk);
        }
        return builder.build();
    }

    @Override
    public DataType getReturnType() {
        return returnType;
    }

    @Override
    public DataStream<BaseRow> getBoundedStream(StreamExecutionEnvironment streamEnv) {
        return null;
    }

    private String buildLookupQueryTemplate(int[] lookupKeys) {
        if (lookupKeys.length == 0) {
            return null;
        }

        String[] lookupKeyString = new String[lookupKeys.length];
        for (int index=0; index<lookupKeys.length; index++) {
            Preconditions.checkArgument(lookupKeys[index] < columnNames.length,
                    "Lookup Key index out of range");
            lookupKeyString[index] = columnNames[lookupKeys[index]];
        }

        // build the JDBCInputFormat
        String tableName = tableProperties.getString(PostgresOptions.TABLE_NAME);

        // build the JDBCInputFormat according to richTableSchema
        StringBuilder fields = new StringBuilder();
        StringBuilder question = new StringBuilder();
        for (int i = 0; i < columnNames.length; i++) {
            if (i != 0) {
                fields.append(", ");
            }
            fields.append(columnNames[i]);
        }

        for(int i = 0; i < lookupKeyString.length; i++) {
            question.append(lookupKeyString[i]);
            question.append(" = ?");

            if (lookupKeyString.length != 1 && i != lookupKeyString.length -1) {
                question.append(" and ");
            }

        }

        return String.format("SELECT %s FROM %s WHERE %s", fields, tableName, question);
    }

    @Override
    public TableFunction<BaseRow> getLookupFunction(int[] lookupKeys) {
        String queryTemplate = buildLookupQueryTemplate(lookupKeys);
        return new PostgresLookupFunction(createInputFormat(queryTemplate), returnType);
    }

    @Override
    public AsyncTableFunction<BaseRow> getAsyncLookupFunction(int[] lookupKeys) {
        String queryTemplate = buildLookupQueryTemplate(lookupKeys);
        return new PostgresAsyncLookupFunction(createInputFormat(queryTemplate), returnType);
    }

    @Override
    public LookupConfig getLookupConfig() {
        LookupConfig config = new LookupConfig();
        config.setAsyncEnabled(true);
        config.setAsyncTimeoutMs(10000);
        config.setAsyncOutputMode(LookupConfig.AsyncOutputMode.ORDERED);
        return config;
    }

    @Override
    public TableStats getTableStats() {
        return null;
    }

    public String explainSource() {
        return "Postgres";
    }

    public static Builder builder() {
        return new Builder();
    }

    public static class Builder {
        private PostgresTableSource postgresTableSource;
        private LinkedHashMap<String, Tuple2<InternalType, Boolean>> schema = new LinkedHashMap<>();
        private Set<String> primaryKeys = new HashSet<>();
        private Set<Set<String>> uniqueKeys = new HashSet<>();
        private Set<Set<String>> normalIndexes = new HashSet<>();
        TableProperties tableProperties;

        public Set<String> getPrimaryKeys() {
            return primaryKeys;
        }

        public void setPrimaryKeys(Set<String> primaryKeys) {
            this.primaryKeys = primaryKeys;
        }

        public Set<Set<String>> getUniqueKeys() {
            return uniqueKeys;
        }

        public void setUniqueKeys(Set<Set<String>> uniqueKeys) {
            this.uniqueKeys = uniqueKeys;
        }

        public Set<Set<String>> getNormalIndexes() {
            return normalIndexes;
        }

        public void setNormalIndexes(Set<Set<String>> normalIndexes) {
            this.normalIndexes = normalIndexes;
        }

        public TableProperties getTableProperties() {
            return tableProperties;
        }

        public void setTableProperties(TableProperties tableProperties) {
            this.tableProperties = tableProperties;
        }

        public Builder tableProperties(TableProperties tableProperties) {
            this.tableProperties = tableProperties;
            return this;
        }

        public Builder field(String columnName,
                             TypeInformation columnType) {
            if (schema.containsKey(columnName)) {
                throw new IllegalArgumentException("duplicate column: " + columnName);
            }

            InternalType internalType = TypeConverters.createInternalTypeFromTypeInfo(columnType);
            boolean nullable = !FlinkTypeFactory.isTimeIndicatorType(internalType);
            schema.put(columnName, new Tuple2<>(internalType, nullable));
            return this;
        }

        public Builder field(String columnName,
                             InternalType columnType) {
            if (schema.containsKey(columnName)) {
                throw new IllegalArgumentException("duplicate column: " + columnName);
            }

            boolean nullable = !FlinkTypeFactory.isTimeIndicatorType(columnType);
            schema.put(columnName, new Tuple2<>(columnType, nullable));
            return this;
        }

        public Builder field(String columnName,
                              InternalType columnType,
                              boolean nullable) {
            if (schema.containsKey(columnName)) {
                throw new IllegalArgumentException("duplicate column: " + columnName);
            }

            schema.put(columnName, new Tuple2<>(columnType, nullable));
            return this;
        }

        public Builder fields(String[] columnNames,
                              InternalType[] columnTypes,
                              boolean[] nullables){
            for (int index=0; index<columnNames.length; index++) {
                field(columnNames[index], columnTypes[index], nullables[index]);
            }
            return this;
        }

        public PostgresTableSource build() {
            if (schema.isEmpty()) {
                throw new IllegalArgumentException("Postgres Table source fields can't be empty");
            }

            return new PostgresTableSource(
                    tableProperties,
                    schema.keySet().toArray(new String[0]),
                    schema.values()
                            .stream()
                            .map(x -> x.f0)
                            .collect(Collectors.toList())
                            .toArray(new InternalType[0]),
                    schema.values()
                            .stream()
                            .map(x -> x.f1)
                            .collect(Collectors.toList())
                            .toArray(new Boolean[0]),
                    primaryKeys.toArray(new String[0]),
                    uniqueKeys
                            .stream()
                            .map(u -> u.toArray(new String[0]))  // mapping each List to an array
                            .collect(Collectors.toList())               // collecting as a List<String[]>
                            .toArray(new String[uniqueKeys.size()][]),

                    normalIndexes
                            .stream()
                            .map(u -> u.toArray(new String[0]))  // mapping each List to an array
                            .collect(Collectors.toList())               // collecting as a List<String[]>
                            .toArray(new String[normalIndexes.size()][])
            );
        }
    }

}
