package io.infinivision.flink.connectors.hbase

import org.apache.flink.connectors.hbase.table.HBaseValidator
import org.apache.flink.table.descriptors.DescriptorProperties
import scala.collection.JavaConverters._

object HBase121Validator extends HBaseValidator{
  val CONNECTOR_VERSION_VALUE_121 = "1.2.1"
  val CONNECTOR_HBASE_VERSION = "version"
  val CONNECTOR_HBASE_BATCH_SIZE = "batchSize".toLowerCase

  override def validate(properties: DescriptorProperties): Unit = {
    validateVersion(properties)
    validateBatchSize(properties)
  }

  def validateVersion(properties: DescriptorProperties): Unit = {
    val versions = List(CONNECTOR_VERSION_VALUE_121)
    properties.validateEnumValues(CONNECTOR_HBASE_VERSION, false, versions.asJava)
  }

  def validateBatchSize(properties: DescriptorProperties): Unit = {
    properties.validateInt(CONNECTOR_HBASE_BATCH_SIZE, true, 1)
  }

}
