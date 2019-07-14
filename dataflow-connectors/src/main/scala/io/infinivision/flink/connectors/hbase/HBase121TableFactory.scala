package io.infinivision.flink.connectors.hbase

import java.util

import org.apache.flink.api.java.tuple.{Tuple3 => JTuple3}
import org.apache.flink.table.sinks.{BatchCompatibleStreamTableSink, BatchTableSink, StreamTableSink, TableSink}
import org.apache.flink.table.sources.{BatchTableSource, StreamTableSource, TableSource}
import org.apache.flink.types.Row
import java.lang.{Integer => JInteger}

import io.infinivision.flink.connectors.utils.CommonTableOptions
import org.apache.flink.connectors.hbase.table.HBaseTableSchemaV2
import org.apache.flink.connectors.hbase.table.HBaseValidator.{COLUMNFAMILY_QUALIFIER_DELIMITER_PATTERN, CONNECTOR_HBASE_CLIENT_PARAM_PREFIX, CONNECTOR_HBASE_TABLE_NAME, CONNECTOR_TYPE_VALUE_HBASE}
import org.apache.flink.table.api.RichTableSchema
import org.apache.flink.table.api.types.{DataType, TypeConverters}
import org.apache.flink.table.descriptors.ConnectorDescriptorValidator.{CONNECTOR_PROPERTY_VERSION, CONNECTOR_TYPE}
import org.apache.flink.table.factories._
import org.apache.flink.table.util.{Logging, TableProperties}
import org.apache.flink.util.StringUtils
import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.hbase.{HBaseConfiguration, HConstants}

import scala.collection.JavaConverters._

/**
  * HBaseTableFactory implementation for HBase version 1.2.1
  */
class HBase121TableFactory
  extends StreamTableSourceFactory[Row]
    with StreamTableSinkFactory[Row]
    with BatchTableSourceFactory[Row]
    with BatchCompatibleTableSinkFactory[Row]
    with Logging {

  val HBASE_VERSION = "1.2.1"

  def hbaseVersion(): String = HBASE_VERSION

  override def requiredContext(): util.Map[String, String] = {
    val context = new util.HashMap[String, String]()
    context.put(CONNECTOR_TYPE, CONNECTOR_TYPE_VALUE_HBASE)
    context.put(HBase121Validator.CONNECTOR_HBASE_VERSION, hbaseVersion())
    context.put(CONNECTOR_PROPERTY_VERSION, "1")
    context
  }

  override def supportedProperties(): util.List[String] = {
    val supportedProperties = List(CONNECTOR_HBASE_TABLE_NAME,
      CONNECTOR_HBASE_CLIENT_PARAM_PREFIX) ++ HBase121Validator.SUPPORTED_KEYS ++ CommonTableOptions.SUPPORTED_KEYS.asScala
    supportedProperties.asJava
  }

  def preCheck(properties: util.Map[String, String]): Unit = {
    val tableName = properties.get(CONNECTOR_HBASE_TABLE_NAME)
    if (StringUtils.isNullOrWhitespaceOnly(tableName)) {
      throw new RuntimeException("HBase table Name should not be empty")
    }

    val zkQuorum = properties.get(HConstants.ZOOKEEPER_QUORUM)
    if (StringUtils.isNullOrWhitespaceOnly(zkQuorum)) {
      // get from HBase configuration (need to include hbase-site.xml in the classpath)
      val conf = HBaseConfiguration.create()
      val zkQ = conf.get(HConstants.ZOOKEEPER_QUORUM)
      if (StringUtils.isNullOrWhitespaceOnly(zkQ)) {
        throw new RuntimeException("HBase zookeeper quorum should not be empty. please ensure the hbase-site.xml is the current classpath")
      }
    }
  }

  def getTableSchemaFromProperties(properties: util.Map[String, String]): RichTableSchema = {
    val tableProperties = new TableProperties
    tableProperties.putProperties(properties)
    tableProperties.readSchemaFromProperties(null)
  }

  def createClientConfiguration(userParams: util.Map[String, String]): Configuration = {
    val conf = HBaseConfiguration.create()
    if (null != userParams) {
      userParams.asScala.foreach {
        case (k, v) => conf.set(k, v)
      }
    }
    conf
  }

  def extractHBaseSchemaAndIndexMapping(richTableSchema: RichTableSchema): JTuple3[HBaseTableSchemaV2, JInteger, util.List[JInteger]] = {
    val columnNames = richTableSchema.getColumnNames
    val pks = richTableSchema.getPrimaryKeys
    if (null == pks || pks.size() != 1) {
      throw new IllegalArgumentException("HBase table schema must be contain only one primary key for rowKey")
    }

    val rowKey = pks.get(0)
    val rowKeySourceIndex = columnNames.indexOf(rowKey)
    if (-1 == rowKeySourceIndex) {
      throw new IllegalArgumentException("invalid primary key")
    }

    val columnTypes = richTableSchema.getColumnTypes
    val rowKeyType = TypeConverters.createExternalTypeInfoFromDataType(columnTypes(rowKeySourceIndex))
    val hTableSchemaBuilder = new HBaseTableSchemaV2.Builder(rowKey, rowKeyType)
    val qualifierSourceIndexes: util.List[JInteger] = new util.ArrayList[JInteger]()
    for (idx <- 0 until columnNames.length) {
      if (idx != rowKeySourceIndex) {
        val cfq = columnNames(idx).split(COLUMNFAMILY_QUALIFIER_DELIMITER_PATTERN)
        if (cfq.length != 2) {
          throw new IllegalArgumentException(s"invalid column name: ${columnNames(idx)}." +
            s" for HBase the column name pattern should be `columnFamily.qualifier`")
        }
        val columnType = TypeConverters.createExternalTypeInfoFromDataType(columnTypes(idx))
        hTableSchemaBuilder.addColumn(cfq(0), cfq(1), columnType)
        qualifierSourceIndexes.add(idx)
      }
    }

    val hTableSchema = hTableSchemaBuilder.build()
    JTuple3.of(hTableSchema, rowKeySourceIndex, qualifierSourceIndexes)
  }

  def createTableSink(properties: util.Map[String, String]): TableSink[Row] = {
    // validate HBase common options
    validateCommonTableOptions(properties)

    val hTableName = properties.get(CONNECTOR_HBASE_TABLE_NAME)
    // construct the TableProperties
    val tableProperties = new TableProperties
    tableProperties.putProperties(properties)

    LOG.info(s"CreateTableSink TableProperties: $tableProperties")

    val richSchema = tableProperties.readSchemaFromProperties(null)

    val hbaseSchemaInfo = extractHBaseSchemaAndIndexMapping(richSchema)
    val batchSize = tableProperties.getString(HBase121Validator.CONNECTOR_HBASE_BATCH_SIZE).toInt
    new HBase121UpsertTableSink(
      richSchema,
      hTableName,
      hbaseSchemaInfo.f0,
      hbaseSchemaInfo.f1,
      hbaseSchemaInfo.f2,
      createClientConfiguration(properties),
      batchSize)
      .configure(richSchema.getColumnNames, richSchema.getColumnTypes.asInstanceOf[Array[DataType]])
      .asInstanceOf[TableSink[Row]]
  }

  def createTableSource(properties: util.Map[String, String]): TableSource = {
    // validate HBase common options
    validateCommonTableOptions(properties)

    val hTableName = properties.get(CONNECTOR_HBASE_TABLE_NAME)
    val conf = HBaseConfiguration.create()
    properties.put(HConstants.ZOOKEEPER_QUORUM, conf.get(HConstants.ZOOKEEPER_QUORUM))
    Option(conf.get(HBase121Validator.HBASE_REGIONSERVER_KERBEROS_PRINCIPAL))
      .foreach(e => properties.put(HBase121Validator.ASYNC_KERBEROS_REGIONSERVER_PRINCIPAL.key(), e))

    // construct the TableProperties
    val tableProperties = new TableProperties
    tableProperties.putProperties(properties)

    LOG.info(s"CreateTableSource TableProperties: $tableProperties")

    val richTableSchema = tableProperties.readSchemaFromProperties(null)
    val hbaseSchemaInfo = extractHBaseSchemaAndIndexMapping(richTableSchema)
    new HBase121TableSource(
      tableProperties,
      richTableSchema,
      hTableName,
      hbaseSchemaInfo.f0,
      hbaseSchemaInfo.f1,
      hbaseSchemaInfo.f2,
      conf
    )

  }

  def validateCommonTableOptions(properties: util.Map[String, String]): Unit = {
    val validator = new HBase121Validator
    validator.validateTableOptions(properties)
  }

  //  override def createBatchTableSink(properties: util.Map[String, String]): BatchTableSink[Row] = {
  //    createTableSink(properties).asInstanceOf[BatchTableSink[Row]]
  //  }
  override def createBatchCompatibleTableSink(properties: util.Map[String, String]): BatchCompatibleStreamTableSink[Row] = {
    createTableSink(properties).asInstanceOf[BatchCompatibleStreamTableSink[Row]]
  }


  override def createBatchTableSource(properties: util.Map[String, String]): BatchTableSource[Row] = {
    createTableSource(properties).asInstanceOf[BatchTableSource[Row]]
  }

  override def createStreamTableSink(properties: util.Map[String, String]): StreamTableSink[Row] = {
    createTableSink(properties).asInstanceOf[StreamTableSink[Row]]
  }

  override def createStreamTableSource(properties: util.Map[String, String]): StreamTableSource[Row] = {
    createTableSource(properties).asInstanceOf[StreamTableSource[Row]]
  }

}
