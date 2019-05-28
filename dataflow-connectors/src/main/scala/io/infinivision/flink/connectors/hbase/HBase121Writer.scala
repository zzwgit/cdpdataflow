package io.infinivision.flink.connectors.hbase

import java.util
import java.lang.{Boolean => JBool, Integer => JInteger}
import java.util.Collections

import org.apache.flink.api.java.tuple.Tuple2
import org.apache.flink.configuration
import org.apache.flink.connectors.hbase.streaming.HBaseWriterBase
import org.apache.flink.connectors.hbase.table.HBaseTableSchemaV2
import org.apache.flink.connectors.hbase.util.HBaseBytesSerializer
import org.apache.flink.streaming.api.checkpoint.ListCheckpointed
import org.apache.flink.table.util.Logging
import org.apache.flink.types.Row
import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.hbase.client.{Delete, Mutation, Put}
import scala.collection.JavaConverters._

class HBase121Writer(
  hbaseTableName: String,
  hbaseSchema: HBaseTableSchemaV2,
  rowKeySourceIndex: Int,
  qualifierSourceIndexes: util.List[JInteger],
  hbaseConfiguration: Configuration)
  extends HBaseWriterBase[Tuple2[JBool, Row]](
    hbaseTableName,
    hbaseSchema,
    hbaseConfiguration)
  with ListCheckpointed[util.ArrayList[Mutation]]
  with Logging{

  val batchSize = 1000
  var batchCounter = 0
  val pendingPuts: util.List[Put] = new util.ArrayList[Put]()
  val pendingDeletes: util.List[Delete] = new util.ArrayList[Delete]()

  val restorePuts: util.List[Put] = new util.ArrayList[Put]()
  val restoreDeletes: util.List[Delete] = new util.ArrayList[Delete]()

  val qualifierList = hbaseSchema.getFamilySchema.getFlatByteQualifiers
  val charset = hbaseSchema.getFamilySchema.getStringCharset
  val inputFieldSerializers: util.List[HBaseBytesSerializer] = new util.ArrayList[HBaseBytesSerializer]()
  val totalQualifiers = hbaseSchema.getFamilySchema.getTotalQualifiers

  for (index <- 0 to totalQualifiers) {
    if (index == rowKeySourceIndex) {
      inputFieldSerializers.add(new HBaseBytesSerializer(hbaseSchema.getRowKeyType, charset))
    } else {
      val typeInfo = if (index < rowKeySourceIndex) qualifierList.get(index)  else qualifierList.get(index-1)
      inputFieldSerializers.add(new HBaseBytesSerializer(typeInfo.f2, charset))
    }

  }

  override def snapshotState(checkpointId: Long, timestamp: Long): util.List[util.ArrayList[Mutation]] = {
    val ops: util.ArrayList[Mutation] = new util.ArrayList[Mutation]()
    ops.addAll(pendingPuts)
    ops.addAll(pendingDeletes)
    Collections.singletonList(ops)
  }

  override def restoreState(state: util.List[util.ArrayList[Mutation]]): Unit = {
    restorePuts.clear()
    restoreDeletes.clear()

    state.asScala.foreach( s =>
      s.asScala.foreach {
        case put: Put => restorePuts.add(put)
        case delete: Delete => restoreDeletes.add(delete)
      }
    )

    if (restorePuts.size() > 0) {
      table.put(restorePuts)
    }

    if (restoreDeletes.size() > 0) {
      table.delete(restoreDeletes)
    }
  }

  override def open(parameters: configuration.Configuration): Unit = {
    super.open(parameters)
  }

  override def invoke(input: Tuple2[JBool, Row]): Unit = {
    val row = input.f1
    if (null == row){
      return
    }

    if (row.getArity != totalQualifiers+1) {
      throw new IllegalArgumentException(s"illegal row: $row")
    }

    val rowKey = inputFieldSerializers.get(rowKeySourceIndex).toHBaseBytes(row.getField(rowKeySourceIndex))
    if(input.f0) {
      // upsert row
      val put = new Put(rowKey)
      for (index <- 0 to totalQualifiers) {
        if (index != rowKeySourceIndex) {
          val qualifierSrcIndex = if (index > rowKeySourceIndex) index - 1 else index
          val qualifierInfo = qualifierList.get(qualifierSrcIndex)
          val qualifierIndex = qualifierSourceIndexes.get(qualifierSrcIndex)
          val value = inputFieldSerializers.get(index).toHBaseBytes(row.getField(qualifierIndex))
          put.addColumn(qualifierInfo.f0, qualifierInfo.f1, value)
        }
      }

      if (batchCounter < batchSize) {
        pendingPuts.add(put)
        batchCounter += 1
      } else {
        flush()
      }
    } else {
      // delete row
      val delete = new Delete(rowKey)
      for (index <- 0 to totalQualifiers) {
        if (index != rowKeySourceIndex) {
          val qualifierSrcIndex = if (index > rowKeySourceIndex) index - 1 else index
          val qualifierInfo = qualifierList.get(qualifierSrcIndex)
          delete.addColumn(qualifierInfo.f0, qualifierInfo.f1)
        }
      }

      if (batchCounter < batchSize) {
        pendingDeletes.add(delete)
        batchCounter += 1
      } else {
        flush()
      }
    }
  }

  def flush(): Unit = {
    table.put(pendingPuts)
    pendingPuts.clear()
    table.delete(pendingDeletes)
    pendingDeletes.clear()
  }

  override def close(): Unit = {
    flush()
    super.close()
  }

  override def toString: String = {
    s"${this.getClass.getSimpleName} -> table: $hbaseTableName schema: {${hbaseSchema.toString}}"
  }
}
