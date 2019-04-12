package io.infinivision.flink.connectors

import org.apache.flink.table.api.types.InternalType
import org.apache.flink.types.Row
import java.lang.{Boolean => JBool, Double => JDouble, Long => JLong}
import java.lang.{Byte => JByte, Float => JFloat, Short => JShort}
import java.sql.{Array => JArray, Date => JDate, Time => JTime, Timestamp => JTimestamp}
import java.math.{BigDecimal => JBigDecimal}

class JDBCAppendOutputFormat(
  private val userName: String,
  private val password: String,
  private val driverName: String,
  private val driverVersion: String,
  private val dbURL: String,
  private val tableName: String,
  private val fieldNames: Array[String],
  private val fieldTypes: Array[InternalType])
  extends JDBCBaseOutputFormat(
    userName,
    password,
    driverName,
    driverVersion,
    dbURL,
    tableName,
    fieldNames,
    fieldTypes) {

  override def prepareSql: String = {
    val selectPlaceholder = Array.fill[String](fieldNames.length)("?").mkString(",")

    s"""
      |INSERT INTO $tableName VALUES ($selectPlaceholder)
    """.stripMargin
  }


  override def updatePreparedStatement(row: Row): Unit = {
    for (index <- 0 until row.getArity) {
      val field = row.getField(index)
      field match {
        case f: String =>
          statement.setString(index + 1, f)
        case f: JLong =>
          statement.setLong(index + 1, f)
        case f: JBigDecimal =>
          statement.setBigDecimal(index + 1, f)
        case f: Integer =>
          statement.setInt(index + 1, f)
        case f: JDouble =>
          statement.setDouble(index + 1, f)
        case f: JBool =>
          statement.setBoolean(index+1, f)
        case f: JFloat =>
          statement.setFloat(index+1, f)
        case f: JShort =>
          statement.setShort(index+1, f)
        case f: JByte =>
          statement.setByte(index+1, f)
        case f: JArray =>
          statement.setArray(index+1, f)
        case f: JDate =>
          statement.setDate(index+1, f)
        case f: JTime =>
          statement.setTime(index+1, f)
        case f: JTimestamp =>
          statement.setTimestamp(index+1, f)
        case _ =>
          statement.setObject(index+1, field)
          LOG.error(s"illegal row field type: ${field.getClass.getSimpleName}")
      }
    }
  }
}