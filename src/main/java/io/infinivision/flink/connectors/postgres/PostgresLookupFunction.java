package io.infinivision.flink.connectors.postgres;


import org.apache.flink.api.java.io.jdbc.JDBCInputFormat;
import org.apache.flink.table.api.RichTableSchema;
import org.apache.flink.table.api.functions.FunctionContext;
import org.apache.flink.table.api.functions.TableFunction;
import org.apache.flink.table.api.types.DataType;
import org.apache.flink.table.util.TableProperties;
import org.apache.flink.types.Row;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class PostgresLookupFunction extends TableFunction<Row> {
    private static final Logger LOG = LoggerFactory.getLogger(PostgresLookupFunction.class);
    public static final String DRIVERNAME = "org.postgresql.Driver";


    private JDBCInputFormat inputFormat;
    private TableProperties tableProperties;
    private RichTableSchema richTableSchema;
    private String[] indexKeys;

    public PostgresLookupFunction(JDBCInputFormat inputFormat) {
        this.inputFormat = inputFormat;
    }

    public PostgresLookupFunction(String[] indexKeys, RichTableSchema richTableSchema, TableProperties tableProperties) {
        this.indexKeys = indexKeys;
        this.richTableSchema = richTableSchema;
    }

    @Override
    public void open(FunctionContext context) throws Exception {
        super.open(context);

        String username = tableProperties.getString(PostgresOptions.USER_NAME);
        String tablename = tableProperties.getString(PostgresOptions.TABLE_NAME);
        String password = tableProperties.getString(PostgresOptions.PASSWORD);
        String dbURL = tableProperties.getString(PostgresOptions.DB_URL);

        // build the JDBCInputFormat according to richTableSchema
        StringBuilder fields = new StringBuilder();
        StringBuilder question = new StringBuilder();
        String[] columnNames = richTableSchema.getColumnNames();
        for (int i = 0; i < columnNames.length; i++) {
            if (i != 0) {
                fields.append(", ");
                question.append(", ");
            }
            fields.append(columnNames[i]);
            question.append("?");
        }

        this.inputFormat = JDBCInputFormat.buildJDBCInputFormat()
                .setUsername(username)
                .setPassword(password)
                .setDrivername(DRIVERNAME)
                .setDBUrl(dbURL)
                .setRowTypeInfo(richTableSchema.getResultTypeInfo())
                .finish();
    }

    public void eval(Object... values) {

    }

    @Override
    public DataType getResultType(Object[] arguments, Class[] argTypes) {
        return this.richTableSchema.getResultType();
    }
}
