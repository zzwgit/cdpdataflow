package io.infinivision.flink.connectors

import java.lang.{Boolean => JBool, Double => JDouble, Long => JLong}
import java.lang.{Byte => JByte, Float => JFloat, Short => JShort}
import java.sql.{Array => JArray, Date => JDate, Time => JTime, Timestamp => JTimestamp}
import java.sql.{Connection, DriverManager, PreparedStatement, SQLException}
import java.util.{Set => JSet}
import java.math.{BigDecimal => JBigDecimal}

import org.apache.flink.api.common.io.RichOutputFormat
import org.apache.flink.api.java.tuple.{Tuple2 => JTuple2}
import org.apache.flink.configuration.Configuration
import org.apache.flink.table.api.types.InternalType
import org.apache.flink.table.util.Logging
import org.apache.flink.types.Row

import scala.collection.JavaConverters._
import scala.collection.mutable.ArrayBuffer

class JDBCUpsertOutputFormat(
    private val userName: String,
    private val password: String,
    private val driverName: String,
    private val driverVersion: String,
    private val dbURL: String,
    private val tableName: String,
    private val fieldNames: Array[String],
    private val fieldTypes: Array[InternalType],
    private val uniqueKeys: JSet[String])
  extends RichOutputFormat[JTuple2[JBool, Row]]
  with Logging {

  private var dbConn: Connection = _
  private var statement: PreparedStatement = _
  private var batchCount: Int = 0
  private var batchInterval: Int = 5000

  override def configure(parameters: Configuration): Unit = {

  }

  private def establishConnection(): Unit = {
    Class.forName(driverName)
    if (userName == null) dbConn = DriverManager.getConnection(dbURL)
    else dbConn = DriverManager.getConnection(dbURL, userName, password)
  }

  override def open(taskNumber: Int, numTasks: Int): Unit = {
    establishConnection()
    if (dbConn.getMetaData
      .getTables(null, null, tableName, null)
      .next()) {
      // prepare the upsert sql
      val sql = buildUpsertSQL
      LOG.info(s"upsert sql $sql")
      statement = dbConn.prepareStatement(sql)
    } else {
      throw new SQLException(s"table $tableName doesn't exist")
    }
  }

  override def close(): Unit = {
    LOG.info("close JDBCOutputFormat")
    try {
      if (statement != null) {
        LOG.info("flush records")
        flush()
        statement.close()
      }
    } catch {
      case ex: SQLException =>
        LOG.error(s"JDBCUpsertOutputFormat could not be closed: ${ex.getMessage}")
        throw new RuntimeException(ex)
    } finally {
      statement = null
      batchCount = 0
    }

    try {
      if (dbConn != null) {
        dbConn.close()
      }
    } catch {
      case ex: SQLException =>
        LOG.error(s"JDBC Connection could not be closed: ${ex.getMessage}")
        throw new RuntimeException(ex)
    } finally {
      dbConn = null
    }
  }

  /**
    * before 9.5
    * with tests as (update pg_sink set phone_num='123' where id=6 returning *)
    * insert into pg_sink select 1,'test','553780043@qq.com', '123445', 123000,123000 where not exists (select 1 from tests);
    *
    *
    * after 9.5
    *
    *
    * @return upsert sql statement
    */
  private def buildUpsertSQL: String = {
    // build set placeholder
    val setPlaceHolder = fieldNames.foldLeft(ArrayBuffer[String]())(
      (buffer, field) => {
        if (!uniqueKeys.contains(field)) {
          buffer += field + "=?"
        }
        buffer
      }
    ).mkString(",")

    // where placeholder
    val conditionPlaceHolder = uniqueKeys.asScala.map( _ + "=?").mkString(" and ")

    // select placeholder
    val selectPlaceholder = Array.fill[String](fieldNames.length)("?").mkString(",")

    // build SQL
    if (driverVersion.toDouble >= 9.5) {
      s"""
         |
       """.stripMargin
    } else {
      s"""
         | WITH upserts as (UPDATE $tableName set $setPlaceHolder WHERE $conditionPlaceHolder RETURNING *)
         | INSERT INTO $tableName SELECT $selectPlaceholder WHERE NOT EXISTS (SELECT 1 FROM upserts)
       """.stripMargin
    }
  }


  override def writeRecord(record: JTuple2[JBool, Row]): Unit = {
    if (record.f0) {
      // write upsert record
      updatePreparedStatement(record.f1)
      statement.addBatch()
      batchCount += 1
      if (batchCount >= batchInterval) {
        flush()
      }
    } else {
      // do nothing so far
    }

  }

  def flush(): Unit ={
    statement.executeBatch()
    batchCount = 0
  }

  private def updatePreparedStatement(row: Row): Unit = {
    val updateFieldIndex = fieldNames
      .filter { !uniqueKeys.contains(_)}
      .map { fieldNames.indexOf(_) }
    val conditionFieldIndex = uniqueKeys.asScala.toArray.map { fieldNames.indexOf(_) }
    val fieldSize = row.getArity

    for (index <- 0 until row.getArity) {
      val field = row.getField(index)
      field match {
        case f: String =>
          if (updateFieldIndex.contains(index)) {
            statement.setString(updateFieldIndex.indexOf(index) + 1, f)
          }
          if (conditionFieldIndex.contains(index)) {
            statement.setString(updateFieldIndex.length + conditionFieldIndex.indexOf(index) +1, f)
          }
          statement.setString(fieldSize+index+1, f)
        case f: JLong =>
          if (updateFieldIndex.contains(index)) {
            statement.setLong(updateFieldIndex.indexOf(index) + 1, f)
          }
          if (conditionFieldIndex.contains(index)) {
            statement.setLong(updateFieldIndex.length + conditionFieldIndex.indexOf(index) +1, f)
          }
          statement.setLong(fieldSize + index + 1, f)
        case f: JBigDecimal =>
          if (updateFieldIndex.contains(index)) {
            statement.setBigDecimal(updateFieldIndex.indexOf(index) + 1, f)
          }
          if (conditionFieldIndex.contains(index)) {
            statement.setBigDecimal(updateFieldIndex.length + conditionFieldIndex.indexOf(index) +1, f)
          }
          statement.setBigDecimal(fieldSize+index+1, f)
        case f: Integer =>
          if (updateFieldIndex.contains(index)) {
            statement.setInt(updateFieldIndex.indexOf(index) + 1, f)
          }
          if (conditionFieldIndex.contains(index)) {
            statement.setInt(updateFieldIndex.length + conditionFieldIndex.indexOf(index) +1, f)
          }
          statement.setInt(fieldSize+index+1, f)
        case f: JDouble =>
          if (updateFieldIndex.contains(index)) {
            statement.setDouble(updateFieldIndex.indexOf(index) + 1, f)
          }
          if (conditionFieldIndex.contains(index)) {
            statement.setDouble(updateFieldIndex.length + conditionFieldIndex.indexOf(index) +1, f)
          }
          statement.setDouble(fieldSize+index+1, f)
        case f: JBool =>
          if (updateFieldIndex.contains(index)) {
            statement.setBoolean(updateFieldIndex.indexOf(index) + 1, f)
          }
          if (conditionFieldIndex.contains(index)) {
            statement.setBoolean(updateFieldIndex.length + conditionFieldIndex.indexOf(index) +1, f)
          }
          statement.setBoolean(fieldSize+index+1, f)
        case f: JFloat =>
          if (updateFieldIndex.contains(index)) {
            statement.setFloat(updateFieldIndex.indexOf(index) + 1, f)
          }
          if (conditionFieldIndex.contains(index)) {
            statement.setFloat(updateFieldIndex.length + conditionFieldIndex.indexOf(index) +1, f)
          }
          statement.setFloat(fieldSize+index+1, f)
        case f: JShort =>
          if (updateFieldIndex.contains(index)) {
            statement.setShort(updateFieldIndex.indexOf(index) + 1, f)
          }
          if (conditionFieldIndex.contains(index)) {
            statement.setShort(updateFieldIndex.length + conditionFieldIndex.indexOf(index) +1, f)
          }
          statement.setShort(fieldSize+index+1, f)
        case f: JByte =>
          if (updateFieldIndex.contains(index)) {
            statement.setByte(updateFieldIndex.indexOf(index) + 1, f)
          }
          if (conditionFieldIndex.contains(index)) {
            statement.setByte(updateFieldIndex.length + conditionFieldIndex.indexOf(index) +1, f)
          }
          statement.setByte(fieldSize+index+1, f)
        case f: JArray =>
          if (updateFieldIndex.contains(index)) {
            statement.setArray(updateFieldIndex.indexOf(index) + 1, f)
          }
          if (conditionFieldIndex.contains(index)) {
            statement.setArray(updateFieldIndex.length + conditionFieldIndex.indexOf(index) +1, f)
          }
          statement.setArray(fieldSize+index+1, f)
        case f: JDate =>
          if (updateFieldIndex.contains(index)) {
            statement.setDate(updateFieldIndex.indexOf(index) + 1, f)
          }
          if (conditionFieldIndex.contains(index)) {
            statement.setDate(updateFieldIndex.length + conditionFieldIndex.indexOf(index) +1, f)
          }
          statement.setDate(fieldSize+index+1, f)
        case f: JTime =>
          if (updateFieldIndex.contains(index)) {
            statement.setTime(updateFieldIndex.indexOf(index) + 1, f)
          }
          if (conditionFieldIndex.contains(index)) {
            statement.setTime(updateFieldIndex.length + conditionFieldIndex.indexOf(index) +1, f)
          }
          statement.setTime(fieldSize+index+1, f)
        case f: JTimestamp =>
          if (updateFieldIndex.contains(index)) {
            statement.setTimestamp(updateFieldIndex.indexOf(index) + 1, f)
          }
          if (conditionFieldIndex.contains(index)) {
            statement.setTimestamp(updateFieldIndex.length + conditionFieldIndex.indexOf(index) +1, f)
          }
          statement.setTimestamp(fieldSize+index+1, f)
        case _ =>
          if (updateFieldIndex.contains(index)) {
            statement.setObject(updateFieldIndex.indexOf(index) + 1, field)
          }
          if (conditionFieldIndex.contains(index)) {
            statement.setObject(updateFieldIndex.length + conditionFieldIndex.indexOf(index) +1, field)
          }
          statement.setObject(fieldSize+index+1, field)
          LOG.error(s"illegal row field type: ${field.getClass.getSimpleName}")
      }
    }
  }

}
