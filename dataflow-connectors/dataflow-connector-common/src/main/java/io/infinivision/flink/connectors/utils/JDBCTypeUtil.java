package io.infinivision.flink.connectors.utils;

import org.apache.flink.table.api.types.ArrayType;
import org.apache.flink.table.api.types.DataTypes;
import org.apache.flink.table.api.types.DecimalType;
import org.apache.flink.table.api.types.InternalType;

import java.sql.Types;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;

public class JDBCTypeUtil {
    private static final Map<InternalType, Integer> TYPE_MAPPING;
    private static final Map<Integer, String> SQL_TYPE_NAMES;

    static {
        HashMap<InternalType, Integer> m = new HashMap<>();
        m.put(DataTypes.STRING, Types.VARCHAR);
        m.put(DataTypes.BOOLEAN, Types.BOOLEAN);
        m.put(DataTypes.BYTE, Types.TINYINT);
        m.put(DataTypes.SHORT, Types.SMALLINT);
        m.put(DataTypes.INT, Types.INTEGER);
        m.put(DataTypes.LONG, Types.BIGINT);
        m.put(DataTypes.FLOAT, Types.FLOAT);
        m.put(DataTypes.DOUBLE, Types.DOUBLE);
        m.put(DataTypes.DATE, Types.DATE);
        m.put(DataTypes.TIME, Types.TIME);
        m.put(DataTypes.TIMESTAMP, Types.TIMESTAMP);
        m.put(DataTypes.BYTE_ARRAY, Types.BINARY);
        TYPE_MAPPING = Collections.unmodifiableMap(m);

        HashMap<Integer, String> names = new HashMap<>();
        names.put(Types.VARCHAR, "VARCHAR");
        names.put(Types.BOOLEAN, "BOOLEAN");
        names.put(Types.TINYINT, "TINYINT");
        names.put(Types.SMALLINT, "SMALLINT");
        names.put(Types.INTEGER, "INTEGER");
        names.put(Types.BIGINT, "BIGINT");
        names.put(Types.FLOAT, "FLOAT");
        names.put(Types.DOUBLE, "DOUBLE");
        names.put(Types.CHAR, "CHAR");
        names.put(Types.DATE, "DATE");
        names.put(Types.TIME, "TIME");
        names.put(Types.TIMESTAMP, "TIMESTAMP");
        names.put(Types.DECIMAL, "DECIMAL");
        names.put(Types.BINARY, "BINARY");
        SQL_TYPE_NAMES = Collections.unmodifiableMap(names);
    }

    private JDBCTypeUtil() {
    }

    public static int typeInformationToSqlType(InternalType type) {

        if (TYPE_MAPPING.containsKey(type)) {
            return TYPE_MAPPING.get(type);
        } else if (type instanceof ArrayType) {
            return Types.ARRAY;
        } else if (type instanceof DecimalType){
            return Types.DECIMAL;
        } else {
            throw new IllegalArgumentException("Unsupported type: " + type);
        }
    }

    public static String getTypeName(int type) {
        return SQL_TYPE_NAMES.get(type);
    }

    public static String getTypeName(InternalType type) {
        return SQL_TYPE_NAMES.get(typeInformationToSqlType(type));
    }

}

