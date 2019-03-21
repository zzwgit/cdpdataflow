package io.infinivision.flink.connectors.postgres;

import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.TableSchema;
import org.apache.flink.table.api.functions.AsyncTableFunction;
import org.apache.flink.table.api.functions.TableFunction;
import org.apache.flink.table.api.types.DataType;
import org.apache.flink.table.plan.stats.TableStats;
import org.apache.flink.table.sources.BatchTableSource;
import org.apache.flink.table.sources.LookupConfig;
import org.apache.flink.table.sources.LookupableTableSource;
import org.apache.flink.table.sources.StreamTableSource;
import org.apache.flink.types.Row;

public class PostgresTableSource implements
        StreamTableSource<Row>,
        BatchTableSource<Row>,
        LookupableTableSource<Row> {

    @Override
    public DataStream<Row> getDataStream(StreamExecutionEnvironment execEnv) {
        return null;
    }

    @Override
    public TableSchema getTableSchema() {
        return null;
    }

    @Override
    public DataType getReturnType() {
        return null;
    }

    @Override
    public DataStream<Row> getBoundedStream(StreamExecutionEnvironment streamEnv) {
        return null;
    }

    @Override
    public TableFunction<Row> getLookupFunction(int[] lookupKeys) {
        return null;
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

}
