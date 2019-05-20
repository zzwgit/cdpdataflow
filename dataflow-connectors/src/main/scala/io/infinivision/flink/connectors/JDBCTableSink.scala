package io.infinivision.flink.connectors

import org.apache.flink.table.sinks.{BatchCompatibleStreamTableSink, TableSinkBase, UpsertStreamTableSink}
import java.lang.{Boolean => JBool}
import java.util.{Set => JSet}

import org.apache.flink.api.java.tuple.{Tuple2 => JTuple2}
import org.apache.flink.streaming.api.datastream.{DataStream, DataStreamSink}
import org.apache.flink.table.api.RichTableSchema
import org.apache.flink.table.api.types.{DataType, DataTypes}
import org.apache.flink.table.util.{Logging, TableConnectorUtil}
import org.apache.flink.types.Row

class JDBCTableSink(
  outputFormat: JDBCBaseOutputFormat)
  extends TableSinkBase[JTuple2[JBool, Row]]
    with UpsertStreamTableSink[Row]
    with BatchCompatibleStreamTableSink[JTuple2[JBool, Row]]
    with Logging {

  override protected def copy: TableSinkBase[JTuple2[JBool, Row]] = {
    new JDBCTableSink(outputFormat)
  }

  override def emitDataStream(dataStream: DataStream[JTuple2[JBool, Row]]): DataStreamSink[_] = {
    dataStream.addSink(new JDBCTableSinkFunction(outputFormat))
      .name(TableConnectorUtil.generateRuntimeName(getClass, getFieldNames))
  }

  override def emitBoundedStream(boundedStream: DataStream[JTuple2[JBool, Row]]): DataStreamSink[_] = {
    boundedStream.addSink(new JDBCTableSinkFunction(outputFormat))
      .name(TableConnectorUtil.generateRuntimeName(getClass, getFieldNames))
  }

  override def setIsAppendOnly(isAppendOnly: JBool): Unit = {}

  override def setKeyFields(keys: Array[String]): Unit = {}

  override def getRecordType: DataType = DataTypes.createRowType(getFieldTypes, getFieldNames)


}

object JDBCTableSink {

  class Builder {
    private var userName: String = _
    private var password: String = _
    private var driverName: String = _
    private var driverVersion: String = _
    private var dbURL: String = _
    private var tableName: String = _
    private var primaryKeys: Option[JSet[String]] = None
    private var uniqueKeys: Option[JSet[JSet[String]]] = None
    private var schema: Option[RichTableSchema] = None
    private var updateMode: String = _

    def userName(userName: String): Builder = {
      this.userName = userName
      this
    }

    def password(password: String): Builder = {
      this.password = password
      this
    }

    def driverName(driverName: String): Builder = {
      this.driverName = driverName
      this
    }

    def driverVersion(driverVersion: String): Builder = {
      this.driverVersion = driverVersion
      this
    }

    def dbURL(dbURL: String): Builder = {
      this.dbURL = dbURL
      this
    }

    def tableName(tableName: String): Builder = {
      this.tableName = tableName
      this
    }

    def primaryKeys(primaryKeys: Option[JSet[String]]): Builder = {
      this.primaryKeys = primaryKeys
      this
    }

    def uniqueKeys(uniqueKeys: Option[JSet[JSet[String]]]): Builder = {
      this.uniqueKeys = uniqueKeys
      this
    }

    def schema(schema: Option[RichTableSchema]): Builder = {
      this.schema = schema
      this
    }

    def updateMode(updateMode: String): Builder = {
      this.updateMode = updateMode
      this
    }

    def build(): JDBCTableSink = {
      // check condition
      if (schema.isEmpty) {
        throw new IllegalArgumentException("table schema can not be null")
      }

      val outputFormat: JDBCBaseOutputFormat = if (updateMode == "append") {
        new JDBCAppendOutputFormat(
          userName,
          password,
          driverName,
          driverVersion,
          dbURL,
          tableName,
          schema.get.getColumnNames,
          schema.get.getColumnTypes
        )
      } else {

        // validate index keys
        val uniqueIndex = if (primaryKeys.isEmpty && uniqueKeys.isEmpty) {
          throw new IllegalArgumentException("JDBCUpsertTableSink should at least contain one primary key or one unique index")
        } else if (primaryKeys.isDefined){
          primaryKeys.get
        } else {
          if(uniqueKeys.get.size != 1) {
            throw new IllegalArgumentException("JDBCUpsertTableSink should contain only one unique index")
          }

          if (uniqueKeys.get.size == schema.get.getColumnNames.length) {
            throw new IllegalArgumentException("JDBCUpsertTableSink unique key size should less than total column size")
          }

          uniqueKeys.get.iterator.next
        }

        new JDBCUpsertOutputFormat(
          userName,
          password,
          driverName,
          driverVersion,
          dbURL,
          tableName,
          schema.get.getColumnNames,
          schema.get.getColumnTypes,
          uniqueIndex
        )
      }

      new JDBCTableSink(outputFormat)
    }
  }

  def builder(): Builder = new Builder
}