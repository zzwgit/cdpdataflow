package io.infinivision.flink.connectors.clickhouse;

import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.JSONArray;
import com.google.common.collect.Maps;
import io.infinivision.flink.connectors.jdbc.JDBCBaseOutputFormat;
import org.apache.commons.lang3.ArrayUtils;
import org.apache.commons.lang3.text.StrSubstitutor;
import org.apache.flink.types.Row;
import ru.yandex.clickhouse.ClickHousePreparedStatementImpl;
import scala.Option;

import java.lang.reflect.Method;
import java.sql.Array;
import java.util.Arrays;
import java.util.HashMap;
import java.util.Map;
import java.util.stream.Collectors;

public class ClickHouseAppendOutputFormat extends JDBCBaseOutputFormat {

	private String userName;
	private String password;
	private String driverName;
	private String driverVersion;
	private String dbURL;
	private String tableName;
	private String[] fieldNames;
	private int[] fieldSQLTypes;
	private int batchSize;
	private String[] arrayFields;
	private transient Method setBind;
	private boolean[] isArrayField;
	private Map<Character, String> escapes;

	public ClickHouseAppendOutputFormat(String userName, String password, String driverName, String driverVersion, String dbURL, String tableName, String[] fieldNames, int[] fieldSQLTypes, int batchSize, String[] arrayFields, String[] servers, boolean asyncFlush) {
		super(userName, password, driverName, driverVersion, dbURL, tableName, fieldNames, fieldSQLTypes, ArrayUtils.isEmpty(servers) ? Option.empty() : Option.apply(servers), asyncFlush);
		this.userName = userName;
		this.password = password;
		this.driverName = driverName;
		this.driverVersion = driverVersion;
		this.dbURL = dbURL;
		this.tableName = tableName;
		this.fieldNames = fieldNames;
		this.fieldSQLTypes = fieldSQLTypes;
		this.batchSize = batchSize;
		this.batchInterval(batchSize);
		this.arrayFields = arrayFields;
		this.isArrayField = new boolean[fieldNames.length];
		for (int i = 0; i < fieldNames.length; i++) {
			if (ArrayUtils.contains(arrayFields, fieldNames[i])) {
				isArrayField[i] = true;
			}
		}
		this.escapes = new HashMap<>();
		this.escapes.put('\\', "\\\\");
		this.escapes.put('\n', "\\n");
		this.escapes.put('\t', "\\t");
		this.escapes.put('\b', "\\b");
		this.escapes.put('\f', "\\f");
		this.escapes.put('\r', "\\r");
		this.escapes.put('\0', "\\0");
		this.escapes.put('\'', "\\'");
		this.escapes.put('`', "\\`");
	}

	@Override
	public String prepareSql() {
		try {
			this.setBind = ClickHousePreparedStatementImpl.class.getDeclaredMethod("setBind", int.class, String.class);
			this.setBind.setAccessible(true);
		} catch (NoSuchMethodException e) {
			LOG().error("access private setBind method error", e);
			throw new RuntimeException(e);
		}

		String namePlaceholder = Arrays.stream(fieldNames).map(key -> "`" + key + "`").collect(Collectors.joining(","));
		String valuePlaceholder = Arrays.stream(fieldNames).map(key -> "?").collect(Collectors.joining(","));

		Map<String, String> replaceValue = Maps.newHashMap();
		replaceValue.put("tableName", tableName);
		replaceValue.put("namePlaceholder", namePlaceholder);
		replaceValue.put("valuePlaceholder", valuePlaceholder);
		StrSubstitutor strSubstitutor = new StrSubstitutor(replaceValue);

		return strSubstitutor.replace("INSERT INTO ${tableName} (${namePlaceholder}) VALUES (${valuePlaceholder})");
	}

	@Override
	public void updatePreparedStatement(Row row) {

		if (fieldSQLTypes != null && fieldSQLTypes.length > 0 && fieldSQLTypes.length != row.getArity()) {
			LOG().warn("Column SQL types array doesn't match arity of passed Row! Check the passed array...");
		}

		try {
			if (fieldSQLTypes == null) {
				// no types provided
				for (int index = 0; index < row.getArity(); index++) {
					LOG().warn("Unknown column type for column {}. Best effort approach to set its value: {}.", index + 1, row.getField(index));
					statement().setObject(index + 1, row.getField(index));
				}
			} else {

				for (int index = 0; index < row.getArity(); index++) {

					if (row.getField(index) == null) {
						statement().setNull(index + 1, fieldSQLTypes[index]);
					} else {
						switch (fieldSQLTypes[index]) {
							case java.sql.Types.INTEGER:
								statement().setInt(index + 1, (int) row.getField(index));
								break;
							case java.sql.Types.VARCHAR:
							case java.sql.Types.CHAR:
							case java.sql.Types.NCHAR:
							case java.sql.Types.LONGVARCHAR:
							case java.sql.Types.LONGNVARCHAR:
								String value = (String) row.getField(index);
								if (isArrayField[index]) {
									StringBuilder sb = new StringBuilder(value.length() + 5);
									boolean changed = false;
									for (int i = 0; i < value.length(); i++) {
										char c = value.charAt(i);
										if (this.escapes.containsKey(c)) {
											sb.append(this.escapes.get(c));
										} else if (c == '"') { // " may be changed to '
											boolean needChange = value.charAt(i - 1) != '\\';
											sb.append(needChange ? '\'' : c);
											changed |= needChange;
										} else {
											sb.append(c);
										}
									}
									setBind.invoke(statement(), index + 1, value.length() == sb.length() && !changed ? value : sb.toString());
								} else {
									statement().setString(index + 1, value);
								}
								break;
							case java.sql.Types.DECIMAL:
							case java.sql.Types.NUMERIC:
								statement().setBigDecimal(index + 1, (java.math.BigDecimal) row.getField(index));
								break;
							case java.sql.Types.FLOAT:
								statement().setFloat(index + 1, (float) row.getField(index));
								break;
							case java.sql.Types.TIMESTAMP:
								statement().setTimestamp(index + 1, (java.sql.Timestamp) row.getField(index));
								break;
							case java.sql.Types.TINYINT:
								statement().setByte(index + 1, (byte) row.getField(index));
								break;
							case java.sql.Types.SMALLINT:
								statement().setShort(index + 1, (short) row.getField(index));
								break;
							case java.sql.Types.BIGINT:
								statement().setLong(index + 1, (long) row.getField(index));
								break;
							case java.sql.Types.REAL:
								statement().setFloat(index + 1, (float) row.getField(index));
								break;
							case java.sql.Types.DOUBLE:
								statement().setDouble(index + 1, (double) row.getField(index));
								break;
							case java.sql.Types.DATE:
								statement().setDate(index + 1, (java.sql.Date) row.getField(index));
								break;
							case java.sql.Types.TIME:
								statement().setTime(index + 1, (java.sql.Time) row.getField(index));
								break;
							case java.sql.Types.BOOLEAN:
							case java.sql.Types.BIT:
								statement().setBoolean(index + 1, (boolean) row.getField(index));
								break;
							case java.sql.Types.NULL:
								statement().setNull(index + 1, fieldSQLTypes[index]);
								break;
							case java.sql.Types.BINARY:
							case java.sql.Types.VARBINARY:
							case java.sql.Types.LONGVARBINARY:
								//根据with配置中指定需要转换为数组格式存储到ck时，进行相关转换
								if (ArrayUtils.isNotEmpty(arrayFields) && ArrayUtils.contains(arrayFields, fieldNames[index])) {
									byte[] tempValue = (byte[]) row.getField(index);
									JSONArray array = JSON.parseArray(new String(tempValue, "UTF-8"));
									Array dbArray = dbConn().createArrayOf("Object", array.toArray());
									statement().setArray(index + 1, dbArray);
								} else {
									statement().setBytes(index + 1, (byte[]) row.getField(index));
								}
								break;
							default:
								statement().setObject(index + 1, row.getField(index));
								LOG().warn("Unmanaged sql type ({}) for column {}. Best effort approach to set its value: {}.", fieldSQLTypes[index], index + 1, row.getField(index));
						}
					}

				}
			}
		} catch (Throwable e) {
			throw new RuntimeException("Preparation of ClickHouse statement failed.", e);
		}
	}
}
