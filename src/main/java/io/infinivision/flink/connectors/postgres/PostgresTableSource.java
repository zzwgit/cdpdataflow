package io.infinivision.flink.connectors.postgres;

import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.java.io.jdbc.JDBCInputFormat;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.typeutils.RowTypeInfo;
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
import org.apache.flink.table.plan.stats.TableStats;
import org.apache.flink.table.sources.BatchTableSource;
import org.apache.flink.table.sources.LookupConfig;
import org.apache.flink.table.sources.LookupableTableSource;
import org.apache.flink.table.sources.StreamTableSource;
import org.apache.flink.table.util.TableProperties;
import org.apache.flink.types.Row;
import org.apache.flink.util.Preconditions;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.HashMap;
import java.util.Set;
import java.util.stream.Collectors;


public class PostgresTableSource implements
        StreamTableSource<Row>,
        BatchTableSource<Row>,
        LookupableTableSource<Row> {
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
    private RowTypeInfo returnTypeInfo;

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
        this.returnTypeInfo = (RowTypeInfo) TypeConverters.createExternalTypeInfoFromDataType(returnType);
    }


    private JDBCInputFormat createInputFormat() {
        // build the JDBCInputFormat
        String userName = tableProperties.getString(PostgresOptions.USER_NAME);
        String tableName = tableProperties.getString(PostgresOptions.TABLE_NAME);
        String password = tableProperties.getString(PostgresOptions.PASSWORD);
        String dbURL = tableProperties.getString(PostgresOptions.DB_URL);

        // build the JDBCInputFormat according to richTableSchema
        StringBuilder fields = new StringBuilder();
        for (int i = 0; i < columnNames.length; i++) {
            if (i != 0) {
                fields.append(", ");
            }
            fields.append(columnNames[i]);
        }

        LOG.debug(String.format("SELECT %s FROM %s", fields, tableName));

        return JDBCInputFormat.buildJDBCInputFormat()
                .setUsername(userName)
                .setPassword(password)
                .setDrivername(DRIVERNAME)
                .setDBUrl(dbURL)
                .setQuery(String.format("SELECT %s FROM %s", fields, tableName))
                .setRowTypeInfo(returnTypeInfo)
                .finish();
    }

    @Override
    public DataStream<Row> getDataStream(StreamExecutionEnvironment execEnv) {
        return execEnv.createInput(createInputFormat(), returnTypeInfo);
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
    public DataStream<Row> getBoundedStream(StreamExecutionEnvironment streamEnv) {
        return null;
    }

    @Override
    public TableFunction<Row> getLookupFunction(int[] lookupKeys) {
        Preconditions.checkArgument(null != lookupKeys && lookupKeys.length >= 1,
                "Lookup keys should be greater than 1");

        Preconditions.checkArgument(lookupKeys.length < columnNames.length,
                "Lookup Keys number should be less than the len of schema fields");

        String[] lookupKeyString = new String[lookupKeys.length];
        for (int index=0; index<lookupKeys.length; index++) {
            Preconditions.checkArgument(lookupKeys[index] < columnNames.length,
                    "Lookup Key index out of range");
            lookupKeyString[index] = columnNames[lookupKeys[index]];
        }

        // build the JDBCInputFormat
        String userName = tableProperties.getString(PostgresOptions.USER_NAME);
        String tableName = tableProperties.getString(PostgresOptions.TABLE_NAME);
        String password = tableProperties.getString(PostgresOptions.PASSWORD);
        String dbURL = tableProperties.getString(PostgresOptions.DB_URL);

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

        LOG.debug(String.format("SELECT %s FROM %s WHERE %s", fields, tableName, question));

        JDBCInputFormat inputFormat = JDBCInputFormat.buildJDBCInputFormat()
                .setUsername(userName)
                .setPassword(password)
                .setDrivername(DRIVERNAME)
                .setDBUrl(dbURL)
                .setQuery(String.format("SELECT %s FROM %s WHERE %s", fields, tableName, question))
                .setRowTypeInfo(returnTypeInfo)
                .finish();
        return new PostgresLookupFunction(inputFormat, returnType);
    }

    @Override
    public AsyncTableFunction<Row> getAsyncLookupFunction(int[] lookupKeys) {
        return null;
    }

    @Override
    public LookupConfig getLookupConfig() {
        return new LookupConfig();
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
        private HashMap<String, Tuple2<InternalType, Boolean>> schema;
        private Set<String> primaryKeys;
        private Set<Set<String>> uniqueKeys;
        private Set<Set<String>> normalIndexes;
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
