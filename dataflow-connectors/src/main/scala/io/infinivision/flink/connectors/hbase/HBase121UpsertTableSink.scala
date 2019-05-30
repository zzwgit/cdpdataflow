package io.infinivision.flink.connectors.hbase

import java.util

import org.apache.flink.connectors.hbase.table.HBaseTableSchemaV2
import org.apache.flink.table.api.RichTableSchema
import java.lang.{Boolean => JBool, Integer => JInteger}

import org.apache.flink.api.java.tuple.{Tuple2 => JTuple2}
import org.apache.flink.streaming.api.datastream.{DataStream, DataStreamSink}
import org.apache.flink.table.api.types.DataType
import org.apache.flink.table.sinks.{BatchCompatibleStreamTableSink, TableSinkBase, UpsertStreamTableSink}
import org.apache.flink.table.util.Logging
import org.apache.flink.types.Row
import org.apache.hadoop.conf.Configuration

class HBase121UpsertTableSink(
  tableSchema: RichTableSchema,
  hbaseTableName: String,
  hbaseTableSchema: HBaseTableSchemaV2,
  rowKeyIndex: JInteger,
  qualifierSourceIndexes: util.List[JInteger],
  hbaseConfiguration: Configuration,
  batchSize: Option[Int])
  extends TableSinkBase[JTuple2[JBool, Row]]
    with UpsertStreamTableSink[Row]
    with BatchCompatibleStreamTableSink[JTuple2[JBool, Row]]
    with Logging  {

  override def emitDataStream(dataStream: DataStream[JTuple2[JBool, Row]]): DataStreamSink[_] = {
    val hbaseSink = new HBase121Writer(hbaseTableName, hbaseTableSchema, rowKeyIndex, qualifierSourceIndexes, hbaseConfiguration, batchSize)
    dataStream.addSink(hbaseSink).name(hbaseSink.toString)
  }

  override def emitBoundedStream(boundedStream: DataStream[JTuple2[JBool, Row]]): DataStreamSink[_] = {
    val hbaseSink = new HBase121Writer(hbaseTableName, hbaseTableSchema, rowKeyIndex, qualifierSourceIndexes, hbaseConfiguration, batchSize)
    boundedStream.addSink(hbaseSink).name(hbaseSink.toString)
  }

  override def setKeyFields(keys: Array[String]): Unit = {

  }

  override def setIsAppendOnly(isAppendOnly: JBool): Unit = {

  }


  override protected def copy: TableSinkBase[JTuple2[JBool, Row]] = {
    new HBase121UpsertTableSink(
      tableSchema,
      hbaseTableName,
      hbaseTableSchema,
      rowKeyIndex,
      qualifierSourceIndexes,
      hbaseConfiguration,
      batchSize
    )
  }

  override def getRecordType: DataType = {
    tableSchema.getResultRowType
  }
}
