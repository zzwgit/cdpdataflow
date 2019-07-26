package io.infinivision.flink.connectors.http;

import io.infinivision.flink.connectors.jdbc.BaseRowJDBCInputFormat;
import io.infinivision.flink.connectors.postgres.PostgresLookupFunction;
import io.infinivision.flink.connectors.utils.CacheConfig;
import io.infinivision.flink.connectors.utils.CommonTableOptions;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.java.io.jdbc.JDBCOptions;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.TableSchema;
import org.apache.flink.table.api.functions.AsyncTableFunction;
import org.apache.flink.table.api.functions.TableFunction;
import org.apache.flink.table.api.types.DataType;
import org.apache.flink.table.api.types.InternalType;
import org.apache.flink.table.api.types.RowType;
import org.apache.flink.table.api.types.TypeConverters;
import org.apache.flink.table.calcite.FlinkTypeFactory;
import org.apache.flink.table.dataformat.BaseRow;
import org.apache.flink.table.plan.stats.TableStats;
import org.apache.flink.table.sources.BatchTableSource;
import org.apache.flink.table.sources.LookupConfig;
import org.apache.flink.table.sources.LookupableTableSource;
import org.apache.flink.table.sources.StreamTableSource;
import org.apache.flink.table.typeutils.BaseRowTypeInfo;
import org.apache.flink.table.util.TableProperties;
import org.apache.flink.types.Row;
import org.apache.flink.util.Preconditions;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.HashSet;
import java.util.LinkedHashMap;
import java.util.Set;
import java.util.stream.Collectors;


public class HttpTableSource implements
		StreamTableSource<BaseRow>,
		BatchTableSource<BaseRow>,
		LookupableTableSource<BaseRow> {
	private static final Logger LOG = LoggerFactory.getLogger(HttpTableSource.class);

	private TableProperties tableProperties;
	private String[] columnNames;
	private InternalType[] columnTypes;
	private Boolean[] columnNullables;

	private String[] primaryKeys;
	private String[][] uniqueKeys;
	private String[][] normalIndexes;

	private RowType returnType;
	private BaseRowTypeInfo returnTypeInfo;

	public HttpTableSource(
			TableProperties tableProperties,
			String[] columnNames,
			InternalType[] columnTypes,
			Boolean[] columnNullables,
			String[] primaryKeys,
			String[][] uniqueKeys,
			String[][] normalIndexes) {
		this.tableProperties = tableProperties;
		this.columnNames = columnNames;
		this.columnTypes = columnTypes;
		this.columnNullables = columnNullables;
		this.primaryKeys = primaryKeys;
		this.uniqueKeys = uniqueKeys;
		this.normalIndexes = normalIndexes;

		// return type
		this.returnType = new RowType(columnTypes, columnNames);
		this.returnTypeInfo = TypeConverters.toBaseRowTypeInfo(returnType);
	}

	@Override
	public TableSchema getTableSchema() {
		TableSchema.Builder builder = TableSchema.builder();

		for (int index = 0; index < columnNames.length; index++) {
			builder.column(columnNames[index], columnTypes[index], columnNullables[index]);
		}

		if(primaryKeys.length > 0) {
			builder.primaryKey(primaryKeys);
		}

		for (String[] uk : uniqueKeys) {
			builder.uniqueIndex(uk);
		}

		for (String[] nk : normalIndexes) {
			builder.normalIndex(nk);
		}
		return builder.build();
	}

	@Override
	public DataType getReturnType() {
		return returnType;
	}



	/**
	 * config the table properties
	 *
	 * @param tableProperties The table properties
	 */
	public void configuration(TableProperties tableProperties) {
		this.tableProperties = tableProperties;
	}

	@Override
	public TableFunction<BaseRow> getLookupFunction(int[] lookupKeys) {
		throw new UnsupportedOperationException("http lookup function not suppert sync.");
	}

	@Override
	public AsyncTableFunction<BaseRow> getAsyncLookupFunction(int[] lookupKeys) {
		return new HttpAsyncLookupFunction(tableProperties, returnType);
	}

	@Override
	public LookupConfig getLookupConfig() {
		LookupConfig config = new LookupConfig();
		String mode = tableProperties.getString(CommonTableOptions.MODE);
		boolean isAsync = false;
		if (mode.equalsIgnoreCase(CommonTableOptions.JOIN_MODE.ASYNC.name())) {
			isAsync = true;
		}

		if (isAsync) {
			config.setAsyncEnabled(true);
			String timeout = tableProperties.getString(CommonTableOptions.TIMEOUT);
			config.setAsyncTimeoutMs(Integer.valueOf(timeout));

			String capacity = tableProperties.getString(CommonTableOptions.BUFFER_CAPACITY);
			config.setAsyncBufferCapacity(Integer.valueOf(capacity));
			config.setAsyncOutputMode(LookupConfig.AsyncOutputMode.ORDERED);
		}

		// TODO cache stuff
		return config;
	}

	@Override
	public TableStats getTableStats() {
		return null;
	}

	public String explainSource() {
		return "Http ";
	}

	public static Builder builder() {
		return new Builder();
	}

	@Override
	public DataStream<BaseRow> getBoundedStream(StreamExecutionEnvironment streamEnv) {
		throw new UnsupportedOperationException("http getBoundedStream.");
	}

	@Override
	public DataStream<BaseRow> getDataStream(StreamExecutionEnvironment execEnv) {
		throw new UnsupportedOperationException("http getDataStream.");
	}

	public static class Builder {
		private HttpTableSource httpTableSource;
		private LinkedHashMap<String, Tuple2<InternalType, Boolean>> schema = new LinkedHashMap<>();
		private Set<String> primaryKeys = new HashSet<>();
		private Set<Set<String>> uniqueKeys = new HashSet<>();
		private Set<Set<String>> normalIndexes = new HashSet<>();
		TableProperties tableProperties;

		public Set<String> getPrimaryKeys() {
			return primaryKeys;
		}

		public void setPrimaryKeys(Set<String> primaryKeys) {
			this.primaryKeys = primaryKeys;
		}

		public Set<Set<String>> getUniqueKeys() {
			return uniqueKeys;
		}

		public void setUniqueKeys(Set<Set<String>> uniqueKeys) {
			this.uniqueKeys = uniqueKeys;
		}

		public Set<Set<String>> getNormalIndexes() {
			return normalIndexes;
		}

		public void setNormalIndexes(Set<Set<String>> normalIndexes) {
			this.normalIndexes = normalIndexes;
		}

		public TableProperties getTableProperties() {
			return tableProperties;
		}

		public void setTableProperties(TableProperties tableProperties) {
			this.tableProperties = tableProperties;
		}

		public Builder tableProperties(TableProperties tableProperties) {
			this.tableProperties = tableProperties;
			return this;
		}

		public Builder field(String columnName,
							 TypeInformation columnType) {
			if (schema.containsKey(columnName)) {
				throw new IllegalArgumentException("duplicate column: " + columnName);
			}

			InternalType internalType = TypeConverters.createInternalTypeFromTypeInfo(columnType);
			boolean nullable = !FlinkTypeFactory.isTimeIndicatorType(internalType);
			schema.put(columnName, new Tuple2<>(internalType, nullable));
			return this;
		}

		public Builder field(String columnName,
							 InternalType columnType) {
			if (schema.containsKey(columnName)) {
				throw new IllegalArgumentException("duplicate column: " + columnName);
			}

			boolean nullable = !FlinkTypeFactory.isTimeIndicatorType(columnType);
			schema.put(columnName, new Tuple2<>(columnType, nullable));
			return this;
		}

		public Builder field(String columnName,
							 InternalType columnType,
							 boolean nullable) {
			if (schema.containsKey(columnName)) {
				throw new IllegalArgumentException("duplicate column: " + columnName);
			}

			schema.put(columnName, new Tuple2<>(columnType, nullable));
			return this;
		}

		public Builder fields(String[] columnNames,
							  InternalType[] columnTypes,
							  boolean[] nullables) {
			for (int index = 0; index < columnNames.length; index++) {
				field(columnNames[index], columnTypes[index], nullables[index]);
			}
			return this;
		}

		public HttpTableSource build() {
			if (schema.isEmpty()) {
				throw new IllegalArgumentException("Table source fields can't be empty");
			}

			return new HttpTableSource(
					tableProperties,
					schema.keySet().toArray(new String[0]),
					schema.values()
							.stream()
							.map(x -> x.f0)
							.collect(Collectors.toList())
							.toArray(new InternalType[0]),
					schema.values()
							.stream()
							.map(x -> x.f1)
							.collect(Collectors.toList())
							.toArray(new Boolean[0]),
					primaryKeys.toArray(new String[0]),
					uniqueKeys
							.stream()
							.map(u -> u.toArray(new String[0]))  // mapping each List to an array
							.collect(Collectors.toList())               // collecting as a List<String[]>
							.toArray(new String[uniqueKeys.size()][]),

					normalIndexes
							.stream()
							.map(u -> u.toArray(new String[0]))  // mapping each List to an array
							.collect(Collectors.toList())               // collecting as a List<String[]>
							.toArray(new String[normalIndexes.size()][])
			);
		}
	}

}
