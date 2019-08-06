package io.infinivision.flink.connectors.hbase


import java.util
import java.lang.{Boolean => JBool, Integer => JInteger}
import org.apache.flink.addons.hbase.{AbstractTableInputFormat, HBaseRowInputFormat}
import org.apache.flink.api.common.typeinfo.TypeInformation
import org.apache.flink.api.java.typeutils.ResultTypeQueryable
import org.apache.flink.configuration
import org.apache.flink.connectors.hbase.table.{HBaseTableSchema, HBaseTableSchemaV2}
import org.apache.flink.connectors.hbase.util.{HBaseBytesSerializer, HBaseConfigurationUtil, HBaseTypeUtils}
import org.apache.flink.table.dataformat.{BaseRow, GenericRow}
import org.apache.flink.table.util.Logging
import org.apache.flink.util.StringUtils
import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.hbase.client.{ConnectionFactory, HTable, Result, Scan}
import org.apache.hadoop.hbase.{HBaseConfiguration, HConstants, TableName}
import scala.collection.JavaConverters._

class HBase121RowInputFormat(
  hbaseConfiguration: Configuration,
  tableName: String,
  hbaseTableSchema: HBaseTableSchemaV2,
  rowKeyIndex: Int,
  qualifierSourceIndexes: util.List[JInteger],
  returnType: TypeInformation[BaseRow])
  extends AbstractTableInputFormat[BaseRow]
  with ResultTypeQueryable[BaseRow]
  with Logging {

  private val serializedConfig = HBaseConfigurationUtil.serializeConfiguration(hbaseConfiguration)
  private val qualifierList = hbaseTableSchema.getFamilySchema.getFlatByteQualifiers
  private val charset = hbaseTableSchema.getFamilySchema.getStringCharset
  private val inputFieldSerializers = new util.ArrayList[HBaseBytesSerializer]()
  private val totalQualifiers = hbaseTableSchema.getFamilySchema.getTotalQualifiers

  private val rowKeyInternalTypeIndex = HBaseTypeUtils.getTypeIndex(hbaseTableSchema.getRowKeyType)

  for (index <- 0 to totalQualifiers) {
    if (index == rowKeyIndex) {
      inputFieldSerializers.add(new HBaseBytesSerializer(hbaseTableSchema.getRowKeyType, charset))
    } else {
      val typeInfo = if (index < rowKeyIndex) qualifierList.get(index)  else qualifierList.get(index-1)
      inputFieldSerializers.add(new HBaseBytesSerializer(typeInfo.f2, charset))
    }

  }
  // reused row

  override def getScanner: Scan = {
    val scan = new Scan()
    qualifierList.asScala.foreach( q => {
      scan.addColumn(q.f0, q.f1)
    })
    scan
  }

  override def configure(parameters: configuration.Configuration): Unit = {
    LOG.info(s"configure ${this.getClass.getSimpleName}")
    val runtimeConfig = HBaseConfigurationUtil.deserializeConfiguration(serializedConfig, HBaseConfiguration.create())
    // validate zookeeper quorum
    if (StringUtils.isNullOrWhitespaceOnly(runtimeConfig.get(HConstants.ZOOKEEPER_QUORUM))) {
      throw new RuntimeException(s"missing ${HConstants.ZOOKEEPER_QUORUM} when connect to HBase table")
    }

    // connect to HBase
    val conn = ConnectionFactory.createConnection(runtimeConfig)
    this.table = conn.getTable(TableName.valueOf(tableName)).asInstanceOf[HTable]
    this.scan = getScanner
  }

  override def getTableName: String = tableName

  override def mapResultToOutType(result: Result): BaseRow = {
    val resultRow = new GenericRow(totalQualifiers+1)
    // deserialize rowkey
    val rowKey = inputFieldSerializers.get(rowKeyIndex).fromHBaseBytes(result.getRow)
    resultRow.update(rowKeyIndex, rowKey)
    qualifierList.asScala.zipWithIndex.foreach {
      case (qf, idx) =>
        val qualifierSrcIdx = qualifierSourceIndexes.get(idx)
        val value = result.getValue(qf.f0, qf.f1)
        resultRow.update(qualifierSrcIdx, inputFieldSerializers.get(qualifierSrcIdx).fromHBaseBytes(value))
    }

    resultRow
  }

  override def getProducedType: TypeInformation[BaseRow] = {
    returnType
  }
}
