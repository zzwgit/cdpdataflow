package io.infinivision.flink.connectors.hbase

import java.util
import java.lang.{Boolean => JBool, Integer => JInteger}
import java.util.Collections

import org.apache.flink.api.common.typeinfo.TypeInformation
import org.apache.flink.api.java.tuple.{Tuple2 => JTuple2, Tuple3 => JTuple3}
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
  hbaseConfiguration: Configuration,
  batchSize: Int)
  extends HBaseWriterBase[JTuple2[JBool, Row]](
    hbaseTableName,
    hbaseSchema,
    hbaseConfiguration)
  with ListCheckpointed[util.ArrayList[Mutation]]
  with Logging{

  LOG.info(s"HBase121Writer batchSize: $batchSize")
  var batchCounter = 0
  var pendingPuts: util.List[Put] = new util.ArrayList[Put](batchSize)
  var pendingDeletes: util.List[Delete] = new util.ArrayList[Delete](batchSize)

  val restorePuts: util.List[Put] = new util.ArrayList[Put]()
  val restoreDeletes: util.List[Delete] = new util.ArrayList[Delete]()

  val qualifierList: util.List[JTuple3[Array[Byte], Array[Byte], TypeInformation[_]]] = hbaseSchema.getFamilySchema.getFlatByteQualifiers
  val charset: String = hbaseSchema.getFamilySchema.getStringCharset
  val inputFieldSerializers: util.List[HBaseBytesSerializer] = new util.ArrayList[HBaseBytesSerializer]()
  val totalQualifiers: Int = hbaseSchema.getFamilySchema.getTotalQualifiers

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
    LOG.info("client buffer size ={}", table.getConfiguration.get("hbase.client.write.buffer"))
  }

  override def invoke(input: JTuple2[JBool, Row]): Unit = {
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

      // if current rowkey == last rowkey  replace last
      if (pendingPuts.size() > 0) {
        val lastPut = pendingPuts.get(pendingPuts.size() - 1)
        if (java.util.Arrays.asList(lastPut.getRow:_*).equals(java.util.Arrays.asList(rowKey:_*))) {
          pendingPuts.set(pendingPuts.size() - 1, put)
          return
        }
      }
      pendingPuts.add(put)
      batchCounter += 1
      if (batchCounter >= batchSize) {
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

      pendingDeletes.add(delete)
      batchCounter += 1
      if (batchCounter >= batchSize) {
        flush()
      }
    }
  }

  def flush(): Unit = {
    // flush pending puts
    val prev = System.currentTimeMillis()
    table.put(pendingPuts)
    val post = System.currentTimeMillis()
    if(post-prev> 1000) {
      LOG.info(s"flush $batchSize records to hbase cost ${post-prev}ms")
    }
    pendingPuts.clear()
    // flush pending deletes
    table.delete(pendingDeletes)
    pendingDeletes.clear()

    // reset batch counter
    batchCounter=0
  }

  override def close(): Unit = {
    LOG.info("close HBase121Writer and flush pending PUT/DELETE operation")
    flush()
    super.close()
  }

  override def toString: String = {
    s"${this.getClass.getSimpleName} -> table: $hbaseTableName schema: {${hbaseSchema.toString}}"
  }
}
