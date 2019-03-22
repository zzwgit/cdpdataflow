package io.infinivision.flink.connectors.postgres;

import org.apache.flink.api.java.io.jdbc.JDBCInputFormat;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.RichTableSchema;
import org.apache.flink.table.api.TableSchema;
import org.apache.flink.table.api.functions.AsyncTableFunction;
import org.apache.flink.table.api.functions.TableFunction;
import org.apache.flink.table.api.types.DataType;
import org.apache.flink.table.api.types.InternalType;
import org.apache.flink.table.plan.stats.TableStats;
import org.apache.flink.table.sources.BatchTableSource;
import org.apache.flink.table.sources.LookupConfig;
import org.apache.flink.table.sources.LookupableTableSource;
import org.apache.flink.table.sources.StreamTableSource;
import org.apache.flink.table.util.TableProperties;
import org.apache.flink.types.Row;
import org.apache.flink.util.Preconditions;

import java.util.HashMap;
import java.util.HashSet;
import java.util.Set;


public class PostgresTableSource implements
        StreamTableSource<Row>,
        BatchTableSource<Row>,
        LookupableTableSource<Row> {

    private RichTableSchema richTableSchema;
    private TableProperties tableProperties;
    private Set<String> primaryKeys;
    private Set<Set<String>> uniqueKeys;
    private Set<Set<String>> normalIndexes;

    public PostgresTableSource(TableProperties tableProperties) {
        this.tableProperties = tableProperties;
        this.richTableSchema = this.tableProperties.readSchemaFromProperties(Thread.currentThread().getContextClassLoader());
    }

    @Override
    public DataStream<Row> getDataStream(StreamExecutionEnvironment execEnv) {
        return null;
    }

    @Override
    public TableSchema getTableSchema() {
        TableSchema.Builder builder = TableSchema.builder();

        return null;
    }

    @Override
    public DataType getReturnType() {
        return richTableSchema.getResultType();
    }

    @Override
    public DataStream<Row> getBoundedStream(StreamExecutionEnvironment streamEnv) {
        return null;
    }

    @Override
    public TableFunction<Row> getLookupFunction(int[] lookupKeys) {
        Preconditions.checkArgument(null != lookupKeys && lookupKeys.length >= 1,
                "Lookup keys should be greater than 1");

        Preconditions.checkArgument(lookupKeys.length < richTableSchema.getColumnNames().length,
                "Lookup Keys number should be less than the len of schema fields");

        String[] columnNames = richTableSchema.getColumnNames();
        String[] indexKeys = new String[lookupKeys.length];
        for (int index=0; index<lookupKeys.length; index++) {
            Preconditions.checkArgument(lookupKeys[index] < columnNames.length, "Lookup Key index out of range");
            indexKeys[index] = columnNames[lookupKeys[index]];
        }

        return new PostgresLookupFunction(indexKeys, richTableSchema, tableProperties);
    }

    @Override
    public AsyncTableFunction<Row> getAsyncLookupFunction(int[] lookupKeys) {
        return null;
    }

    @Override
    public LookupConfig getLookupConfig() {
        return null;
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

        private Builder field(String columnName,
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
            return postgresTableSource;
        }
    }

}
