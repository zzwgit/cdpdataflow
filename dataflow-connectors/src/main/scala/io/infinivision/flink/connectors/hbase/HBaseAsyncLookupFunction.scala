package io.infinivision.flink.connectors.hbase

import java.util
import java.lang.{Boolean => JBool, Integer => JInteger}
import java.util.Collections

import com.stumbleupon.async.Callback
import org.apache.flink.api.java.tuple.{Tuple2 => JTuple2, Tuple3 => JTuple3}
import org.apache.flink.api.common.typeinfo.TypeInformation
import org.apache.flink.connectors.hbase.table.HBaseTableSchemaV2
import org.apache.flink.connectors.hbase.util.{HBaseBytesSerializer, HBaseConfigurationUtil, HBaseTypeUtils}
import org.apache.flink.streaming.api.scala.async.ResultFuture
import org.apache.flink.table.api.RichTableSchema
import org.apache.flink.table.api.functions.{AsyncTableFunction, FunctionContext}
import org.apache.flink.table.api.types.DataType
import org.apache.flink.table.util.Logging
import org.apache.flink.types.Row
import org.apache.flink.util.StringUtils
import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.hbase.{HBaseConfiguration, HConstants}
import org.hbase.async.{GetRequest, HBaseClient, KeyValue}

import scala.collection.JavaConverters._

class HBaseAsyncLookupFunction(
  tableSchema: RichTableSchema,
  hbaseTableName: String,
  hbaseSchema: HBaseTableSchemaV2,
  rowKeySourceIndex: Int,
  qualifierSourceIndexes: util.List[JInteger],
  hbaseConfiguration: Configuration)
  extends AsyncTableFunction[Row]
  with Logging{

  private val qualifierList: util.List[JTuple3[Array[Byte], Array[Byte], TypeInformation[_]]] = hbaseSchema.getFamilySchema.getFlatByteQualifiers
  private val charset: String = hbaseSchema.getFamilySchema.getStringCharset
  private val inputFieldSerializers: util.List[HBaseBytesSerializer] = new util.ArrayList[HBaseBytesSerializer]()
  private val totalQualifiers: Int = hbaseSchema.getFamilySchema.getTotalQualifiers

  private val serializedConfig: Array[Byte] = HBaseConfigurationUtil.serializeConfiguration(hbaseConfiguration)
  private val rowKeyInternalTypeIndex = HBaseTypeUtils.getTypeIndex(hbaseSchema.getRowKeyType)

  // hbase connection
//  protected var hConnection: Connection = _

  // hbase table
//  protected var hTable: HTable = _

  private var hClient: HBaseClient = _

  for (index <- 0 to totalQualifiers) {
    if (index == rowKeySourceIndex) {
      inputFieldSerializers.add(new HBaseBytesSerializer(hbaseSchema.getRowKeyType, charset))
    } else {
      val typeInfo = if (index < rowKeySourceIndex) qualifierList.get(index)  else qualifierList.get(index-1)
      inputFieldSerializers.add(new HBaseBytesSerializer(typeInfo.f2, charset))
    }

  }

  override def open(context: FunctionContext): Unit = {
    LOG.info("start open HBaseAsyncLookupFunction...")
    super.open(context)
    val runtimeConfig = HBaseConfigurationUtil.deserializeConfiguration(serializedConfig, HBaseConfiguration.create)
    val zkQuorum = runtimeConfig.get(HConstants.ZOOKEEPER_QUORUM)

    if (StringUtils.isNullOrWhitespaceOnly(zkQuorum)) {
      throw new RuntimeException(s"can not connect to hbase without {${HConstants.ZOOKEEPER_QUORUM}} configuration")
    }
    hClient = new HBaseClient(zkQuorum)
    LOG.info("end open HBaseAsyncLookupFunction...")
  }

  override def close(): Unit = {
    LOG.info("start close HBaseAsyncLookupFunction...")

    super.close()

    if (null != hClient) {

    }
    LOG.info("end close HBaseAsyncLookupFunction...")

  }

  override def getResultType(arguments: Array[AnyRef], argTypes: Array[Class[_]]): DataType = {
    tableSchema.getResultRowType
  }

  def parseResult(rowKey: AnyRef, cells: util.ArrayList[KeyValue]): Row = {
    val row = new Row(totalQualifiers+1)
    row.setField(rowKeySourceIndex, rowKey)
    cells.asScala.foreach( cell => {
      val family = cell.family()
      val qualifier = cell.qualifier()
      qualifierList.asScala.zipWithIndex.foreach {
        case (qInfo, idx) =>
          if (family.equals(qInfo.f0) && qualifier.equals(qInfo.f1)) {
            val qualifierSrcIdx = qualifierSourceIndexes.get(idx)
            row.setField(qualifierSrcIdx, inputFieldSerializers.get(qualifierSrcIdx).fromHBaseBytes(cell.value()))
          }
      }
    })

    row
  }

  def eval(resultFuture: ResultFuture[Row], rowKey: AnyRef): Unit = {
    val rk = HBaseTypeUtils.serializeFromInternalObject(rowKey, rowKeyInternalTypeIndex, HBaseTypeUtils.DEFAULT_CHARSET)
    val getRequest: GetRequest = new GetRequest(hbaseTableName, rk)
    val defered = hClient.get(getRequest)

    defered.addCallback[Unit]( new Callback[Unit, util.ArrayList[KeyValue]] {
      override def call(cells: util.ArrayList[KeyValue]): Unit = {
        val row = parseResult(rowKey, cells)
        resultFuture.complete(List(row))
      }
    })
  }
}
