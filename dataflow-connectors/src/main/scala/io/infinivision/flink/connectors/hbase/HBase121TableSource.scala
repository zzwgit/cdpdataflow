package io.infinivision.flink.connectors.hbase

import org.apache.flink.connectors.hbase.table.{HBaseLookupFunction, HBaseTableSchemaV2}
import org.apache.flink.streaming.api.datastream.DataStream
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment
import org.apache.flink.table.api.{RichTableSchema, TableSchema}
import org.apache.flink.table.api.functions.{AsyncTableFunction, TableFunction}
import org.apache.flink.table.api.types.{DataType, RowType, TypeConverters}
import org.apache.flink.table.plan.stats.TableStats
import org.apache.flink.table.sources.{BatchTableSource, LookupConfig, LookupableTableSource, StreamTableSource}
import org.apache.flink.types.Row
import java.lang.{Integer => JInteger}
import java.util

import io.infinivision.flink.connectors.utils.CommonTableOptions
import org.apache.flink.api.common.io.InputFormat
import org.apache.flink.core.io.InputSplit
import org.apache.flink.table.dataformat.BaseRow
import org.apache.flink.table.util.{TableProperties, TableSchemaUtil}
import org.apache.hadoop.conf.Configuration

class HBase121TableSource(
  tableProperties: TableProperties,
  tableSchema: RichTableSchema,
  hbaseTableName: String,
  hbaseTableSchema: HBaseTableSchemaV2,
  rowKeyIndex: Int,
  qualifierSourceIndexes: util.List[JInteger],
  hbaseConfiguration: Configuration)
  extends StreamTableSource[BaseRow]
  with BatchTableSource[BaseRow]
  with LookupableTableSource[Row] {

  private val returnType = TypeConverters.toBaseRowTypeInfo(tableSchema.getResultRowType.asInstanceOf[RowType])

  override def getTableSchema: TableSchema = {
    val rowKey = tableSchema.getColumnNames()(rowKeyIndex)
    TableSchemaUtil.builderFromDataType(getReturnType).primaryKey(rowKey).build()
  }

  override def getDataStream(execEnv: StreamExecutionEnvironment): DataStream[BaseRow] = {
    execEnv.createInput(
      new HBase121RowInputFormat(hbaseConfiguration, hbaseTableName, hbaseTableSchema, rowKeyIndex, qualifierSourceIndexes, returnType)
        .asInstanceOf[InputFormat[BaseRow, InputSplit]],
      TypeConverters.toBaseRowTypeInfo(getReturnType.asInstanceOf[RowType]),
      explainSource()
    )
  }

  override def getBoundedStream(streamEnv: StreamExecutionEnvironment): DataStream[BaseRow] = {
    streamEnv.createInput(
      new HBase121RowInputFormat(hbaseConfiguration, hbaseTableName, hbaseTableSchema, rowKeyIndex, qualifierSourceIndexes, returnType)
        .asInstanceOf[InputFormat[BaseRow, InputSplit]],
      TypeConverters.toBaseRowTypeInfo(getReturnType.asInstanceOf[RowType]),
      explainSource()
    )
  }

  override def getLookupFunction(lookupKeys: Array[Int]): TableFunction[Row] = {
    if (lookupKeys == null || lookupKeys.length != 1 || rowKeyIndex != lookupKeys(0)) {
      throw new RuntimeException("HBase table can only be join on RowKey for now")
    }
    new HBaseLookupFunction(
      tableSchema,
      hbaseTableName,
      hbaseTableSchema,
      rowKeyIndex,
      qualifierSourceIndexes,
      hbaseConfiguration
    )
  }

  override def getAsyncLookupFunction(lookupKeys: Array[Int]): AsyncTableFunction[Row] = {
    new HBaseAsyncLookupFunction(
      tableSchema,
      hbaseTableName,
      hbaseTableSchema,
      rowKeyIndex,
      qualifierSourceIndexes,
      hbaseConfiguration
    )
  }

  override def getLookupConfig: LookupConfig = {
    val lookupConfig = new LookupConfig
    val mode = tableProperties.getString(CommonTableOptions.MODE)
    val isAsync = if (mode.equalsIgnoreCase(CommonTableOptions.JOIN_MODE.ASYNC.name())) true else false
    if (isAsync) {
      lookupConfig.setAsyncEnabled(true)
      val timeout = tableProperties.getString(CommonTableOptions.TIMEOUT).toLong
      val capacity = tableProperties.getString(CommonTableOptions.BUFFER_CAPACITY).toInt
      lookupConfig.setAsyncBufferCapacity(capacity)
      lookupConfig.setAsyncTimeoutMs(timeout)
      lookupConfig.setAsyncOutputMode(LookupConfig.AsyncOutputMode.ORDERED)
    }
    lookupConfig
  }

  override def getTableStats: TableStats = {
    super.getTableStats
  }

  override def getReturnType: DataType = {
    tableSchema.getResultRowType
  }

  override def explainSource(): String = {
    s"HBase table: $hbaseTableName, schema: {$getReturnType}"
  }
}
