package io.infinivision.flink.connectors

import org.apache.flink.table.api.types.InternalType
import java.util.{Set => JSet}
import java.lang.{Boolean => JBool, Double => JDouble, Long => JLong}
import java.lang.{Byte => JByte, Float => JFloat, Short => JShort}
import java.sql.{Array => JArray, Date => JDate, Time => JTime, Timestamp => JTimestamp}
import java.math.{BigDecimal => JBigDecimal}

import io.infinivision.flink.connectors.postgres.PostgresValidator
import org.apache.flink.types.Row

import scala.collection.mutable.ArrayBuffer
import scala.collection.JavaConverters._

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
  extends JDBCBaseOutputFormat(
    userName,
    password,
    driverName,
    driverVersion,
    dbURL,
    tableName,
    fieldNames,
    fieldTypes
  ) {

  /** Build the update SQL for upsert operation
    *
    * Postgres handle upsert syntax are different before version 9.5 and after version 9.5(included)
    *
    * CREATE TABLE (
    *   aid varchar,
    *   uid varchar,
    *   label int
    * )
    *
    * before 9.5
    * WITH upserts as (UPDATE train_output set label = 1 WHERE aid='1781' and uid='55796870'  returning *)
    * INSERT INTO train_output SELECT '1781','55796870',1 WHERE NOT EXISTS (SELECT 1 FROM upserts)
    *
    * after 9.5
    * insert into train_output values ('1781', '55796870', 0) on conflict (aid, uid) do update set label = 0
    *
    * @return upsert sql statement
    */
  override def prepareSql: String = {
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
    val selectPlaceholder = fieldNames.map{ _ => "?" }.mkString(",")

    // build SQL
    if (driverVersion.equals(PostgresValidator.CONNECTOR_VERSION_VALUE_95)) {
      s"""
         | INSERT INTO $tableName VALUES ($selectPlaceholder)
         | ON CONFLICT (${uniqueKeys.asScala.mkString(",")}) DO UPDATE SET $setPlaceHolder
       """.stripMargin
    } else {
      s"""
         | WITH upserts as (UPDATE $tableName SET $setPlaceHolder WHERE $conditionPlaceHolder RETURNING *)
         | INSERT INTO $tableName SELECT $selectPlaceholder WHERE NOT EXISTS (SELECT 1 FROM upserts)
       """.stripMargin
    }
  }

  override def updatePreparedStatement(row: Row): Unit = {
    if (driverVersion.equals(PostgresValidator.CONNECTOR_VERSION_VALUE_94)) {
      updatePreparedStatement94(row)
    } else {
      updatePreparedStatement95(row)
    }
  }

  private def updatePreparedStatement94(row: Row): Unit = {
    val updateFieldIndex = fieldNames
      .filter { !uniqueKeys.contains(_) }
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

  private def updatePreparedStatement95(row: Row): Unit = {
    val updateFieldIndex = fieldNames
      .filter { !uniqueKeys.contains(_) }
      .map { fieldNames.indexOf(_) }
    val fieldSize = row.getArity

    for (index <- 0 until row.getArity) {
      val field = row.getField(index)
      field match {
        case f: String =>
          if (updateFieldIndex.contains(index)) {
            statement.setString(fieldSize + updateFieldIndex.indexOf(index) + 1, f)
          }
          statement.setString(index + 1, f)
        case f: JLong =>
          if (updateFieldIndex.contains(index)) {
            statement.setLong(fieldSize + updateFieldIndex.indexOf(index) + 1, f)
          }
          statement.setLong(index + 1, f)
        case f: JBigDecimal =>
          if (updateFieldIndex.contains(index)) {
            statement.setBigDecimal(fieldSize + updateFieldIndex.indexOf(index) + 1, f)
          }
          statement.setBigDecimal(index + 1, f)
        case f: Integer =>
          if (updateFieldIndex.contains(index)) {
            statement.setInt(fieldSize + updateFieldIndex.indexOf(index) + 1, f)
          }
          statement.setInt(index + 1, f)
        case f: JDouble =>
          if (updateFieldIndex.contains(index)) {
            statement.setDouble(fieldSize + updateFieldIndex.indexOf(index) + 1, f)
          }
          statement.setDouble(index + 1, f)
        case f: JBool =>
          if (updateFieldIndex.contains(index)) {
            statement.setBoolean(fieldSize + updateFieldIndex.indexOf(index) + 1, f)
          }
          statement.setBoolean(index+1, f)
        case f: JFloat =>
          if (updateFieldIndex.contains(index)) {
            statement.setFloat(fieldSize + updateFieldIndex.indexOf(index) + 1, f)
          }
          statement.setFloat(index+1, f)
        case f: JShort =>
          if (updateFieldIndex.contains(index)) {
            statement.setShort(fieldSize + updateFieldIndex.indexOf(index) + 1, f)
          }
          statement.setShort(index+1, f)
        case f: JByte =>
          if (updateFieldIndex.contains(index)) {
            statement.setByte(fieldSize + updateFieldIndex.indexOf(index) + 1, f)
          }
          statement.setByte(index+1, f)
        case f: JArray =>
          if (updateFieldIndex.contains(index)) {
            statement.setArray(fieldSize + updateFieldIndex.indexOf(index) + 1, f)
          }
          statement.setArray(index+1, f)
        case f: JDate =>
          if (updateFieldIndex.contains(index)) {
            statement.setDate(fieldSize + updateFieldIndex.indexOf(index) + 1, f)
          }
          statement.setDate(index+1, f)
        case f: JTime =>
          if (updateFieldIndex.contains(index)) {
            statement.setTime(fieldSize + updateFieldIndex.indexOf(index) + 1, f)
          }
          statement.setTime(index+1, f)
        case f: JTimestamp =>
          if (updateFieldIndex.contains(index)) {
            statement.setTimestamp(fieldSize + updateFieldIndex.indexOf(index) + 1, f)
          }
          statement.setTimestamp(index+1, f)
        case _ =>
          if (updateFieldIndex.contains(index)) {
            statement.setObject(fieldSize + updateFieldIndex.indexOf(index) + 1, field)
          }
          statement.setObject(index+1, field)
          LOG.error(s"illegal row field type: ${field.getClass.getSimpleName}")
      }
    }
  }


}
