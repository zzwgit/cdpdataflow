package io.infinivision.flink.connectors.postgres;

import io.infinivision.flink.connectors.jdbc.BaseRowJDBCInputFormat;
import org.apache.flink.core.io.InputSplit;
import org.apache.flink.streaming.api.functions.async.ResultFuture;
import org.apache.flink.table.api.functions.AsyncTableFunction;
import org.apache.flink.table.api.functions.FunctionContext;
import org.apache.flink.table.api.types.DataType;
import org.apache.flink.table.api.types.RowType;
import org.apache.flink.table.dataformat.BaseRow;
import org.apache.flink.table.dataformat.GenericRow;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.Collections;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;


public class PostgresAsyncLookupFunction extends AsyncTableFunction<BaseRow> {
    private static final Logger LOG = LoggerFactory.getLogger(PostgresAsyncLookupFunction.class);

    private final BaseRowJDBCInputFormat inputFormat;
    private final RowType returnType;
    private ExecutorService executor;
    private InputSplit inputSplit;

    public PostgresAsyncLookupFunction(BaseRowJDBCInputFormat inputFormat, RowType returnType) {
        this.inputFormat = inputFormat;
        this.returnType = returnType;
    }

    @Override
    public void open(FunctionContext context) throws Exception {
        super.open(context);
        inputFormat.openInputFormat();
        inputSplit = inputFormat.createInputSplits(1)[0];
        executor = Executors.newCachedThreadPool();
    }

    public void eval(final ResultFuture<BaseRow> asyncCollector, Object... values) {
        CompletableFuture
                .supplyAsync(() -> {
                    ResultSet rs;
                    try {
                        rs = inputFormat.executeQuery(values);
                    } catch (SQLException ex) {
                        throw new CompletionException(ex);
                    }
                    return rs;
                }, executor)
                .thenAccept((resultSet) -> {
                    if (null == resultSet) {
                        return;
                    }
                    boolean hasNext;
                    GenericRow reuseRow = new GenericRow(returnType.getArity());
                    try {
                        hasNext = resultSet.next();
                        while (hasNext) {
                            for (int pos = 0; pos < reuseRow.getArity(); pos++) {
                                reuseRow.update(pos, resultSet.getObject(pos + 1));
                            }
                            asyncCollector.complete(Collections.singleton(reuseRow));
                            hasNext = resultSet.next();
                        }
                    } catch (SQLException ex) {
                        throw new CompletionException( ex);
                    }
                });
    }

    @Override
    public DataType getResultType(Object[] arguments, Class[] argTypes) {
        return this.returnType;
    }

    @Override
    public void close() throws Exception {
        super.close();
        if (executor != null && !executor.isShutdown()) {
            executor.shutdownNow();
        }
    }
}
