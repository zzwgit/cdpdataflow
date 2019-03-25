package io.infinivision.flink.connectors.postgres;


import org.apache.flink.api.java.io.jdbc.JDBCInputFormat;
import org.apache.flink.table.api.functions.FunctionContext;
import org.apache.flink.table.api.functions.TableFunction;
import org.apache.flink.table.api.types.DataType;
import org.apache.flink.table.api.types.RowType;
import org.apache.flink.types.Row;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class PostgresLookupFunction extends TableFunction<Row> {
    private static final Logger LOG = LoggerFactory.getLogger(PostgresLookupFunction.class);

    private JDBCInputFormat inputFormat;
    private RowType returnType;

    public PostgresLookupFunction(JDBCInputFormat inputFormat, RowType returnType) {
        this.inputFormat = inputFormat;
        this.returnType = returnType;
    }


    @Override
    public void open(FunctionContext context) throws Exception {
        super.open(context);
        this.inputFormat.openInputFormat();
    }

    public void eval(Object... values) {
        LOG.debug("eval function. input values: %s", values);

    }

    @Override
    public DataType getResultType(Object[] arguments, Class[] argTypes) {
        return this.returnType;
    }
}
