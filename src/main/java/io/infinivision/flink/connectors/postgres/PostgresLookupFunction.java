package io.infinivision.flink.connectors.postgres;

import io.infinivision.flink.connectors.jdbc.BaseRowJDBCInputFormat;
import org.apache.flink.core.io.InputSplit;
import org.apache.flink.table.api.functions.FunctionContext;
import org.apache.flink.table.api.functions.TableFunction;
import org.apache.flink.table.api.types.DataType;
import org.apache.flink.table.api.types.RowType;
import org.apache.flink.table.dataformat.BaseRow;
import org.apache.flink.table.dataformat.GenericRow;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import java.io.IOException;

public class PostgresLookupFunction extends TableFunction<BaseRow> {
    private static final Logger LOG = LoggerFactory.getLogger(PostgresLookupFunction.class);

    private BaseRowJDBCInputFormat inputFormat;
    private final RowType returnType;

    public PostgresLookupFunction(BaseRowJDBCInputFormat inputFormat, RowType returnType) {
        this.inputFormat = inputFormat;
        this.returnType = returnType;
    }

    @Override
    public void open(FunctionContext context) throws Exception {
        super.open(context);
        inputFormat.openInputFormat();
    }

    public void eval(Object... values) throws IOException {
        LOG.debug("eval function. input values: %s", values);
        Object[][] queryParameters = new Object[][] {values};
        inputFormat.setParameterValues(queryParameters);
        InputSplit[] inputSplits = inputFormat.createInputSplits(1);
        inputFormat.open(inputSplits[0]);
        BaseRow row = new GenericRow(returnType.getArity());
        while (true) {
            BaseRow r = inputFormat.nextRecord(row);
            if (r == null) {
                break;
            }
            collect(r);
        }
    }

    @Override
    public DataType getResultType(Object[] arguments, Class[] argTypes) {
        return this.returnType;
    }

    @Override
    public void close() throws Exception {
        super.close();
    }
}
