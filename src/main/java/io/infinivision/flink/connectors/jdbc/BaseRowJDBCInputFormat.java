package io.infinivision.flink.connectors.jdbc;

import org.apache.flink.api.common.io.DefaultInputSplitAssigner;
import org.apache.flink.api.common.io.RichInputFormat;
import org.apache.flink.api.common.io.statistics.BaseStatistics;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.java.io.jdbc.split.ParameterValuesProvider;
import org.apache.flink.api.java.typeutils.ResultTypeQueryable;
import org.apache.flink.api.java.typeutils.RowTypeInfo;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.core.io.GenericInputSplit;
import org.apache.flink.core.io.InputSplit;
import org.apache.flink.core.io.InputSplitAssigner;
import org.apache.flink.table.dataformat.BaseRow;
import org.apache.flink.table.dataformat.BinaryString;
import org.apache.flink.table.dataformat.GenericRow;
import org.apache.flink.util.Preconditions;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.math.BigDecimal;
import java.sql.*;
import java.util.Arrays;

public class BaseRowJDBCInputFormat extends RichInputFormat<BaseRow, InputSplit> implements ResultTypeQueryable<BaseRow> {
    private static final Logger LOG = LoggerFactory.getLogger(BaseRowJDBCInputFormat.class);

    private String username;
    private String password;
    private String drivername;
    private String dbURL;
    private String queryTemplate;
    private int resultSetType;
    private int resultSetConcurrency;
    private TypeInformation<BaseRow> returnTypeInfo;

    private transient Connection dbConn;
    private transient PreparedStatement statement;
    private transient ResultSet resultSet;
    private transient GenericRow reuseRow;
    private int fetchSize;

    private boolean hasNext;
    private Object[][] parameterValues;

    public BaseRowJDBCInputFormat() {

    }

    @Override
    public void configure(Configuration parameters) {

    }

    // set the parameter values
    public void setParameterValues(Object[][] parameterValues) {
        this.parameterValues = parameterValues;
    }

    @Override
    public void openInputFormat() throws IOException {
        super.openInputFormat();

        //called once per inputFormat (on open)
        try {
            Class.forName(drivername);
            if (username == null) {
                dbConn = DriverManager.getConnection(dbURL);
            } else {
                dbConn = DriverManager.getConnection(dbURL, username, password);
            }
            statement = dbConn.prepareStatement(queryTemplate, resultSetType, resultSetConcurrency);
            if (fetchSize == Integer.MIN_VALUE || fetchSize > 0) {
                statement.setFetchSize(fetchSize);
            }
        } catch (SQLException se) {
            throw new IllegalArgumentException("open() failed." + se.getMessage(), se);
        } catch (ClassNotFoundException cnfe) {
            throw new IllegalArgumentException("JDBC-Class not found. - " + cnfe.getMessage(), cnfe);
        }
    }

    @Override
    public void open(InputSplit inputSplit) throws IOException {
        try {
            if (inputSplit != null && parameterValues != null) {
                for (int i = 0; i < parameterValues[inputSplit.getSplitNumber()].length; i++) {
                    Object param = parameterValues[inputSplit.getSplitNumber()][i];
                    if (param instanceof String) {
                        statement.setString(i + 1, (String) param);
                    } else if (param instanceof BinaryString) {
                        BinaryString bs = (BinaryString)param;
                        statement.setString(i + 1, bs.toString());
                    } else if (param instanceof Long) {
                        statement.setLong(i + 1, (Long) param);
                    } else if (param instanceof Integer) {
                        statement.setInt(i + 1, (Integer) param);
                    } else if (param instanceof Double) {
                        statement.setDouble(i + 1, (Double) param);
                    } else if (param instanceof Boolean) {
                        statement.setBoolean(i + 1, (Boolean) param);
                    } else if (param instanceof Float) {
                        statement.setFloat(i + 1, (Float) param);
                    } else if (param instanceof BigDecimal) {
                        statement.setBigDecimal(i + 1, (BigDecimal) param);
                    } else if (param instanceof Byte) {
                        statement.setByte(i + 1, (Byte) param);
                    } else if (param instanceof Short) {
                        statement.setShort(i + 1, (Short) param);
                    } else if (param instanceof Date) {
                        statement.setDate(i + 1, (Date) param);
                    } else if (param instanceof Time) {
                        statement.setTime(i + 1, (Time) param);
                    } else if (param instanceof Timestamp) {
                        statement.setTimestamp(i + 1, (Timestamp) param);
                    } else if (param instanceof Array) {
                        statement.setArray(i + 1, (Array) param);
                    } else {
                        //extends with other types if needed
                        throw new IllegalArgumentException("open() failed. Parameter " + i + " of type " + param.getClass() + " is not handled (yet).");
                    }
                }
                if (LOG.isDebugEnabled()) {
                    LOG.debug(String.format("Executing '%s' with parameters %s", queryTemplate, Arrays.deepToString(parameterValues[inputSplit.getSplitNumber()])));
                }
            }
            resultSet = statement.executeQuery();
            hasNext = resultSet.next();
        } catch (SQLException se) {
            throw new IllegalArgumentException("open() failed." + se.getMessage(), se);
        }
    }

    @Override
    public boolean reachedEnd() throws IOException {
        return !hasNext;
    }

    @Override
    public BaseRow nextRecord(BaseRow ignore) throws IOException {
        if (reuseRow == null) {
            reuseRow = new GenericRow(ignore.getArity());
        }
        try {
            if (!hasNext) {
                return null;
            }
            for (int pos = 0; pos < reuseRow.getArity(); pos++) {

                reuseRow.update(pos, resultSet.getObject(pos + 1));
            }
            //update hasNext after we've read the record
            hasNext = resultSet.next();
            return reuseRow;
        } catch (SQLException se) {
            throw new IOException("Couldn't read data - " + se.getMessage(), se);
        } catch (NullPointerException npe) {
            throw new IOException("Couldn't access resultSet", npe);
        }
    }

    @Override
    public BaseStatistics getStatistics(BaseStatistics cachedStatistics) throws IOException {
        return cachedStatistics;
    }

    @Override
    public InputSplit[] createInputSplits(int minNumSplits) throws IOException {
        if (parameterValues == null) {
            return new GenericInputSplit[]{new GenericInputSplit(0, 1)};
        }
        GenericInputSplit[] ret = new GenericInputSplit[parameterValues.length];
        for (int i = 0; i < ret.length; i++) {
            ret[i] = new GenericInputSplit(i, ret.length);
        }
        return ret;
    }

    @Override
    public InputSplitAssigner getInputSplitAssigner(InputSplit[] inputSplits) {
        return new DefaultInputSplitAssigner(inputSplits);
    }

    @Override
    public TypeInformation<BaseRow> getProducedType() {
        return returnTypeInfo;
    }

    @Override
    public void close() throws IOException {
        if (resultSet == null) {
            return;
        }
        try {
            resultSet.close();
        } catch (SQLException se) {
            LOG.info("Inputformat ResultSet couldn't be closed - " + se.getMessage());
        }
    }

    @Override
    public void closeInputFormat() throws IOException {
        super.closeInputFormat();
        //called once per inputFormat (on close)
        try {
            if (statement != null) {
                statement.close();
            }
        } catch (SQLException se) {
            LOG.info("Inputformat Statement couldn't be closed - " + se.getMessage());
        } finally {
            statement = null;
        }

        try {
            if (dbConn != null) {
                dbConn.close();
            }
        } catch (SQLException se) {
            LOG.info("Inputformat couldn't be closed - " + se.getMessage());
        } finally {
            dbConn = null;
        }

        parameterValues = null;
    }

    public static Builder buildBaseRowJDBCInputFormat() {
        return new Builder();
    }


    public static class Builder {
        private BaseRowJDBCInputFormat format;

        public Builder() {
            this.format = new BaseRowJDBCInputFormat();
            //using TYPE_FORWARD_ONLY for high performance reads
            this.format.resultSetType = ResultSet.TYPE_FORWARD_ONLY;
            this.format.resultSetConcurrency = ResultSet.CONCUR_READ_ONLY;
        }

        public Builder setUsername(String username) {
            format.username = username;
            return this;
        }

        public Builder setPassword(String password) {
            format.password = password;
            return this;
        }

        public Builder setDrivername(String drivername) {
            format.drivername = drivername;
            return this;
        }

        public Builder setDBUrl(String dbURL) {
            format.dbURL = dbURL;
            return this;
        }

        public Builder setQuery(String query) {
            format.queryTemplate = query;
            return this;
        }

        public Builder setResultSetType(int resultSetType) {
            format.resultSetType = resultSetType;
            return this;
        }

        public Builder setResultSetConcurrency(int resultSetConcurrency) {
            format.resultSetConcurrency = resultSetConcurrency;
            return this;
        }

        public Builder setParametersProvider(ParameterValuesProvider parameterValuesProvider) {
            format.parameterValues = parameterValuesProvider.getParameterValues();
            return this;
        }

        public Builder setRowTypeInfo(TypeInformation<BaseRow> rowTypeInfo) {
            format.returnTypeInfo = rowTypeInfo;
            return this;
        }

        public Builder setFetchSize(int fetchSize) {
            Preconditions.checkArgument(fetchSize == Integer.MIN_VALUE || fetchSize > 0,
                    "Illegal value %s for fetchSize, has to be positive or Integer.MIN_VALUE.", fetchSize);
            format.fetchSize = fetchSize;
            return this;
        }

        public BaseRowJDBCInputFormat finish() {
            if (format.username == null) {
                LOG.info("Username was not supplied separately.");
            }
            if (format.password == null) {
                LOG.info("Password was not supplied separately.");
            }
            if (format.dbURL == null) {
                throw new IllegalArgumentException("No database URL supplied");
            }
            if (format.queryTemplate == null) {
                throw new IllegalArgumentException("No query supplied");
            }
            if (format.drivername == null) {
                throw new IllegalArgumentException("No driver supplied");
            }
            if (format.returnTypeInfo == null) {
                throw new IllegalArgumentException("No " + RowTypeInfo.class.getSimpleName() + " supplied");
            }
            if (format.parameterValues == null) {
                LOG.debug("No input splitting configured (data will be read with parallelism 1).");
            }
            return format;
        }
    }
}
