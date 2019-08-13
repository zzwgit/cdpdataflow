package io.infinivision.flink.connectors.jdbc

import java.lang.{Boolean => JBool}
import java.util.{Set => JSet}

import io.infinivision.flink.connectors.utils.JDBCTypeUtil
import org.apache.flink.api.common.ExecutionConfig
import org.apache.flink.api.java.tuple.{Tuple2 => JTuple2}
import org.apache.flink.streaming.api.datastream.{DataStream, DataStreamSink}
import org.apache.flink.table.api.{RichTableSchema, TableConfig}
import org.apache.flink.table.api.types.{DataType, DataTypes, InternalType}
import org.apache.flink.table.sinks.{BatchCompatibleStreamTableSink, BatchTableSink, TableSinkBase, UpsertStreamTableSink}
import org.apache.flink.table.util.{Logging, TableConnectorUtil}
import org.apache.flink.types.Row

import scala.collection.mutable

abstract class JDBCTableSink(
  outputFormat: JDBCBaseOutputFormat)
  extends TableSinkBase[JTuple2[JBool, Row]]
    with UpsertStreamTableSink[Row]
    with BatchCompatibleStreamTableSink[JTuple2[JBool, Row]]
//    with BatchTableSink[JTuple2[JBool, Row]]
    with Logging {

  override def emitDataStream(dataStream: DataStream[JTuple2[JBool, Row]]): DataStreamSink[_] = {
    dataStream.addSink(new JDBCTableSinkFunction(outputFormat))
      .name(TableConnectorUtil.generateRuntimeName(getClass, getFieldNames))
  }

  override def emitBoundedStream(boundedStream: DataStream[JTuple2[JBool, Row]]): DataStreamSink[_] = {
    boundedStream.addSink(new JDBCTableSinkFunction(outputFormat))
      .name(TableConnectorUtil.generateRuntimeName(getClass, getFieldNames))
  }

//  override def emitBoundedStream(boundedStream: DataStream[JTuple2[JBool, Row]], tableConfig: TableConfig, executionConfig: ExecutionConfig): DataStreamSink[_] = {
//    boundedStream
//      .addSink(new JDBCTableSinkFunction(outputFormat))
//      .name(TableConnectorUtil.generateRuntimeName(this.getClass(), getFieldNames));
//  }

  override def getRecordType: DataType = DataTypes.createRowType(getFieldTypes, getFieldNames)


}


abstract class JDBCTableSinkBuilder {
  protected var userName: String = _
  protected var password: String = _
  protected var driverName: String = _
  protected var driverVersion: String = _
  protected var dbURL: String = _
  protected var tableName: String = _
  protected var primaryKeys: Option[JSet[String]] = None
  protected var uniqueKeys: Option[JSet[JSet[String]]] = None
  protected var schema: Option[RichTableSchema] = None
  protected var updateMode: String = _
  protected var parameterTypes: Array[Int] = _
  protected var batchSize: Int = _
  protected var servers: String = _
  protected var asyncFlush: Boolean = _

  //batchSize为中间变量，最终传递给JDBCBaseOutputFormat的batchInterval
  def batchSize(batchSize: Int): JDBCTableSinkBuilder = {
    this.batchSize = batchSize
    this
  }

  def userName(userName: String): JDBCTableSinkBuilder = {
    this.userName = userName
    this
  }

  def password(password: String): JDBCTableSinkBuilder = {
    this.password = password
    this
  }

  def driverName(driverName: String): JDBCTableSinkBuilder = {
    this.driverName = driverName
    this
  }

  def driverVersion(driverVersion: String): JDBCTableSinkBuilder = {
    this.driverVersion = driverVersion
    this
  }

  def dbURL(dbURL: String): JDBCTableSinkBuilder = {
    this.dbURL = dbURL
    this
  }

  def tableName(tableName: String): JDBCTableSinkBuilder = {
    this.tableName = tableName
    this
  }

  def primaryKeys(primaryKeys: Option[JSet[String]]): JDBCTableSinkBuilder = {
    this.primaryKeys = primaryKeys
    this
  }

  def uniqueKeys(uniqueKeys: Option[JSet[JSet[String]]]): JDBCTableSinkBuilder = {
    this.uniqueKeys = uniqueKeys
    this
  }

  def schema(schema: Option[RichTableSchema]): JDBCTableSinkBuilder = {
    this.schema = schema
    this
  }

  def updateMode(updateMode: String): JDBCTableSinkBuilder = {
    this.updateMode = updateMode
    this
  }

  def servers(serverz: String): JDBCTableSinkBuilder = {
    this.servers = serverz
    this
  }

  def asyncFlush(asyncFlush: Boolean) : JDBCTableSinkBuilder = {
    this.asyncFlush = asyncFlush
    this
  }

  def setParameterTypes(parameterTypes: Array[InternalType]): Unit = {
    val types = new mutable.ArrayBuffer[Int]()

    parameterTypes.map( t => {
      types += JDBCTypeUtil.typeInformationToSqlType(t)
    })

    this.parameterTypes = types.toArray
  }
  def build(): JDBCTableSink

}