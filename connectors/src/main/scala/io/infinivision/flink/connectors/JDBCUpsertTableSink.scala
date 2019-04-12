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

class JDBCUpsertTableSink(
   outputFormat: JDBCUpsertOutputFormat,
   schema: RichTableSchema)
  extends TableSinkBase[JTuple2[JBool, Row]]
  with UpsertStreamTableSink[Row]
  with BatchCompatibleStreamTableSink[JTuple2[JBool, Row]]
  with Logging {

  override protected def copy: TableSinkBase[JTuple2[JBool, Row]] = {
    new JDBCUpsertTableSink(outputFormat, schema)
  }

  override def emitDataStream(dataStream: DataStream[JTuple2[JBool, Row]]): DataStreamSink[_] = {
    dataStream.addSink(new JDBCUpsertSinkFunction(outputFormat))
      .name(TableConnectorUtil.generateRuntimeName(getClass, getFieldNames))
  }

  override def emitBoundedStream(boundedStream: DataStream[JTuple2[JBool, Row]]): DataStreamSink[_] = {
    boundedStream.addSink(new JDBCUpsertSinkFunction(outputFormat))
      .name(TableConnectorUtil.generateRuntimeName(getClass, getFieldNames))
  }

  override def setIsAppendOnly(isAppendOnly: JBool): Unit = {}

  override def setKeyFields(keys: Array[String]): Unit = {}

  override def getRecordType: DataType = DataTypes.createRowType(getFieldTypes, getFieldNames)


}

object JDBCUpsertTableSink {
  class Builder {
    private var userName: String = _
    private var password: String = _
    private var driverName: String = _
    private var driverVersion: String = _
    private var dbURL: String = _
    private var tableName: String = _
    private var uniqueKeys: Option[JSet[String]] = None
    private var schema: RichTableSchema = _

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

    def uniqueKeys(uniqueKeys: Option[JSet[String]]): Builder = {
      this.uniqueKeys = uniqueKeys
      this
    }

    def schema(schema: RichTableSchema): Builder = {
      this.schema = schema
      this
    }

    def build(): JDBCUpsertTableSink = {
      // check condition
      if (schema == null) {
        throw new IllegalArgumentException("table schema can not be null")
      }

      new JDBCUpsertTableSink(
        new JDBCUpsertOutputFormat(
          userName,
          password,
          driverName,
          driverVersion,
          dbURL,
          tableName,
          schema.getColumnNames,
          schema.getColumnTypes,
          uniqueKeys.get
        ),
        schema
      )
    }
  }

  def builder(): Builder = new Builder
}