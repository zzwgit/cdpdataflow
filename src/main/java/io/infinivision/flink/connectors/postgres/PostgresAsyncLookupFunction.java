package io.infinivision.flink.connectors.postgres;

import io.vertx.core.Vertx;
import io.vertx.core.VertxOptions;
import io.vertx.core.json.JsonArray;
import io.vertx.core.json.JsonObject;
import io.vertx.ext.jdbc.JDBCClient;
import io.vertx.ext.sql.SQLClient;
import io.vertx.ext.sql.SQLConnection;
import org.apache.flink.streaming.api.functions.async.ResultFuture;
import org.apache.flink.table.api.functions.AsyncTableFunction;
import org.apache.flink.table.api.functions.FunctionContext;
import org.apache.flink.table.api.types.DataType;
import org.apache.flink.table.api.types.RowType;
import org.apache.flink.table.dataformat.BaseRow;
import org.apache.flink.table.dataformat.BinaryString;
import org.apache.flink.table.dataformat.GenericRow;
import org.apache.flink.table.util.TableProperties;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Collections;

public class PostgresAsyncLookupFunction extends AsyncTableFunction<BaseRow> {
    private static final Logger LOG = LoggerFactory.getLogger(PostgresAsyncLookupFunction.class);

    private final TableProperties tableProperties;
    private final RowType returnType;
    private final String queryTemplate;

    private transient SQLClient SQLClient;

    public final static int DEFAULT_VERTX_EVENT_LOOP_POOL_SIZE = 1;

    public final static int DEFAULT_VERTX_WORKER_POOL_SIZE = Runtime.getRuntime().availableProcessors() * 2;

    public final static int DEFAULT_MAX_DB_CONN_POOL_SIZE = DEFAULT_VERTX_EVENT_LOOP_POOL_SIZE + DEFAULT_VERTX_WORKER_POOL_SIZE;

    public PostgresAsyncLookupFunction(TableProperties tableProperties,
                                       String queryTemplate,
                                       RowType returnType) {
        this.tableProperties = tableProperties;
        this.queryTemplate = queryTemplate;
        this.returnType = returnType;
    }

    @Override
    public void open(FunctionContext context) throws Exception {
        super.open(context);
        JsonObject pgConfig = new JsonObject();
        pgConfig.put("url", tableProperties.getString(PostgresOptions.DB_URL))
                .put("user", tableProperties.getString(PostgresOptions.USER_NAME))
                .put("password", tableProperties.getString(PostgresOptions.PASSWORD))
                .put("driver_class", "org.postgresql.Driver")
                .put("max_pool_size", DEFAULT_MAX_DB_CONN_POOL_SIZE);

        VertxOptions vo = new VertxOptions();
        vo.setEventLoopPoolSize(DEFAULT_VERTX_EVENT_LOOP_POOL_SIZE);
        vo.setWorkerPoolSize(DEFAULT_VERTX_WORKER_POOL_SIZE);
        Vertx vertx = Vertx.vertx(vo);
        this.SQLClient = JDBCClient.createNonShared(vertx, pgConfig);
    }


    public void eval(final ResultFuture<BaseRow> resultFuture, Object... values) {
        JsonArray inputParams = new JsonArray();
        for (Object value : values) {
            if (value instanceof BinaryString) {
                BinaryString input = (BinaryString)value;
                inputParams.add(input.toString());
            } else {
                inputParams.add(value);
            }
        }

        SQLClient.getConnection(conn -> {
            if (conn.failed()) {
                //Treatment failures
                resultFuture.completeExceptionally(conn.cause());
                return;
            }
            final SQLConnection connection = conn.result();
            connection.queryWithParams(queryTemplate, inputParams, rs -> {
                if (rs.failed()) {
//                    LOG.error("can not retrive the data from the datase", rs.cause());
//                    resultFuture.complete(null);
                    throw new RuntimeException(rs.cause());
                }

                int resultSize = rs.result().getResults().size();
                GenericRow reuseRow = new GenericRow(returnType.getArity());

                if (resultSize > 0) {
                    for (JsonArray line : rs.result().getResults()) {
                        for (int pos = 0; pos < reuseRow.getArity(); pos++) {
                            reuseRow.update(pos, line.getValue(pos));
                        }
                        resultFuture.complete(Collections.singleton(reuseRow));
                    }
                } else {
                    resultFuture.complete(Collections.emptyList());
                }

                connection.close(done -> {
                    if (done.failed()) {
                        throw new RuntimeException(done.cause());
                    }
                });

            });
        });

    }

    @Override
    public DataType getResultType(Object[] arguments, Class[] argTypes) {
        return this.returnType;
    }

    @Override
    public void close() throws Exception {
        super.close();
        if (SQLClient != null) {
            SQLClient.close(done -> {
                if (done.failed()) {
                    throw new RuntimeException(done.cause());
                }
            });
        }
    }
}
