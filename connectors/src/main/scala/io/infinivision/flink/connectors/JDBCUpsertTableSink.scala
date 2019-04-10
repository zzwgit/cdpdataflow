package io.infinivision.flink.connectors

import org.apache.flink.table.sinks.{BatchCompatibleStreamTableSink, TableSinkBase, UpsertStreamTableSink}
import java.lang.{Boolean => JBool}
import java.util.{Set => JSet}
import org.apache.flink.api.java.tuple.{Tuple2 => JTuple2}
import org.apache.flink.streaming.api.datastream.{DataStream, DataStreamSink}
import org.apache.flink.table.api.RichTableSchema
import org.apache.flink.table.api.types.{DataType, DataTypes}
import org.apache.flink.table.util.Logging
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
    LOG.debug("emitDataStream")
    null
  }

  override def emitBoundedStream(boundedStream: DataStream[JTuple2[JBool, Row]]): DataStreamSink[_] = {
    null
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
    private var dbURL: String = _
    private var tableName: String = _
    private var primaryKey: Array[String] = _
    private var uniqueKeys: JSet[JSet[String]] = _
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

    def dbURL(dbURL: String): Builder = {
      this.dbURL = dbURL
      this
    }

    def tableName(tableName: String): Builder = {
      this.tableName = tableName
      this
    }

    def uniqueKeys(uniqueKeys: JSet[JSet[String]]): Builder = {
      this.uniqueKeys = uniqueKeys
      this
    }

    def primaryKey(primaryKey: Array[String]): Builder = {
      this.primaryKey = primaryKey
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

      if (primaryKey.length == 0 && uniqueKeys.isEmpty) {
        throw new IllegalArgumentException("JDBC Upsert Table should at least contain a primary key or unique index")
      }

      new JDBCUpsertTableSink(
        new JDBCUpsertOutputFormat(
          userName,
          password,
          driverName,
          dbURL,
          tableName,
          primaryKey,
          uniqueKeys
        ),
        schema
      )
    }
  }

  def builder(): Builder = new Builder
}