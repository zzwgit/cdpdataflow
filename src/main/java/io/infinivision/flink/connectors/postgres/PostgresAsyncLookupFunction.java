package io.infinivision.flink.connectors.postgres;

import org.apache.flink.streaming.api.scala.async.ResultFuture;
import org.apache.flink.table.api.functions.AsyncTableFunction;
import org.apache.flink.types.Row;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class PostgresAsyncLookupFunction extends AsyncTableFunction {
    private static final Logger LOG = LoggerFactory.getLogger(PostgresAsyncLookupFunction.class);

    public void eval(ResultFuture<Row> result, Object... values) {
        LOG.info("PostgresAsyncLookupFunction eval...");
    }
}
