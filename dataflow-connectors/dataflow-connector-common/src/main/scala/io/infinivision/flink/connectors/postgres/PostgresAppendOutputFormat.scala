package io.infinivision.flink.connectors.postgres

import java.lang.{Boolean => JBool, Integer => JInteger, Byte => JByte,
  Double => JDouble, Float => JFloat, Long => JLong, Short => JShort}
import java.sql.{Date => JDate, Time => JTime, Timestamp => JTimestamp}
import java.math.{BigDecimal => JBigDecimal}
import java.sql.Types

import io.infinivision.flink.connectors.jdbc.JDBCBaseOutputFormat
import org.apache.flink.types.Row

class PostgresAppendOutputFormat (
  private val userName: String,
  private val password: String,
  private val driverName: String,
  private val driverVersion: String,
  private val dbURL: String,
  private val tableName: String,
  private val fieldNames: Array[String],
  private val fieldSQLTypes: Array[Int])
extends JDBCBaseOutputFormat (
  userName,
  password,
  driverName,
  driverVersion,
  dbURL,
  tableName,
  fieldNames,
  fieldSQLTypes) {

  override def prepareSql: String = {
    val selectPlaceholder = fieldNames.map{ _ => "?" }.mkString(",")
    s"""
       |INSERT INTO $tableName VALUES ($selectPlaceholder)
    """.stripMargin
  }


  override def updatePreparedStatement(row: Row): Unit = {
    for (index <- 0 until row.getArity) {
      val field = row.getField(index)
      fieldSQLTypes(index) match {
        case Types.VARCHAR =>
          statement.setString(index + 1, field.asInstanceOf[String])
        case Types.BIGINT =>
          statement.setLong(index + 1, field.asInstanceOf[JLong])
        case Types.DECIMAL =>
          statement.setBigDecimal(index + 1, field.asInstanceOf[JBigDecimal])
        case Types.INTEGER =>
          statement.setInt(index + 1, field.asInstanceOf[JInteger])
        case Types.DOUBLE =>
          statement.setDouble(index + 1, field.asInstanceOf[JDouble])
        case Types.BOOLEAN =>
          statement.setBoolean(index+1, field.asInstanceOf[JBool])
        case Types.FLOAT =>
          statement.setFloat(index+1, field.asInstanceOf[JFloat])
        case Types.SMALLINT =>
          statement.setShort(index+1, field.asInstanceOf[JShort])
        case Types.TINYINT =>
          statement.setByte(index+1, field.asInstanceOf[JByte])
        case Types.DATE =>
          statement.setDate(index+1, field.asInstanceOf[JDate])
        case Types.TIME =>
          statement.setTime(index+1, field.asInstanceOf[JTime])
        case Types.TIMESTAMP =>
          statement.setTimestamp(index+1, field.asInstanceOf[JTimestamp])
//        case Types.ARRAY =>
//          statement.setArray(index+1, field)
        case _ =>
          statement.setObject(index+1, field)
      }
    }
  }

}
