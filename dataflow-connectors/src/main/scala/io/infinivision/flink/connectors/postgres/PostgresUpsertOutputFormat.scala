package io.infinivision.flink.connectors.postgres

import java.lang.{Boolean => JBool, Byte => JByte, Double => JDouble, Float => JFloat, Integer => JInteger, Long => JLong, Short => JShort}
import java.sql.{Array => JArray, Date => JDate, Time => JTime, Timestamp => JTimestamp}
import java.math.{BigDecimal => JBigDecimal}
import java.util.{Set => JSet}
import java.sql.Types

import io.infinivision.flink.common.utils.BytesUtil
import io.infinivision.flink.connectors.jdbc.JDBCBaseOutputFormat
import io.infinivision.flink.connectors.utils.JDBCTypeUtil
import org.apache.flink.types.Row

import scala.collection.JavaConverters._
import scala.collection.mutable.ArrayBuffer


class PostgresUpsertOutputFormat (
  private val userName: String,
  private val password: String,
  private val driverName: String,
  private val driverVersion: String,
  private val dbURL: String,
  private val tableName: String,
  private val fieldNames: Array[String],
  private val fieldSQLTypes: Array[Int],
  private val bitmapField: Option[String],
  private val uniqueKeys: JSet[String])
extends JDBCBaseOutputFormat(
  userName,
  password,
  driverName,
  driverVersion,
  dbURL,
  tableName,
  fieldNames,
  fieldSQLTypes) {

  // set the batch count to 1 if bitmapField defined
  // update flink_gp_bitmap SET user_list=rb_or(user_list, rb_build(?)) where uid=?
  if(bitmapField.isDefined) {
    batchInterval = 1
  }

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
    val tbAlias = "tbAlias"
    // build set placeholder
    val setPlaceHolder = fieldNames.foldLeft(ArrayBuffer[String]())(
      (buffer, field) => {
        if (!uniqueKeys.contains(field)) {
          // handle the bitMap field
          if (bitmapField.isDefined && field.equals(bitmapField.get)) {
            buffer += s"$field=rb_or($tbAlias.$field, rb_build(?))"
          } else {
            buffer += s"$field=?"
          }
        }
        buffer
      }
    ).mkString(",")


    // where placeholder
    val conditionPlaceHolder = uniqueKeys.asScala.map( _ + "=?").mkString(" and ")

    // select placeholder
    val selectPlaceholder = fieldNames.foldLeft(ArrayBuffer[String]())(
      (buffer, field) => {
          // handle the bitMap field
          if (bitmapField.isDefined && field.equals(bitmapField.get)) {
            buffer += s"rb_build(?)"
          } else {
            buffer += s"?"
          }
        buffer
      }
    ).mkString(",")


    // build SQL
    /*

     INSERT INTO flink_gp_bitmap as tb(uid, user_list) VALUES (55796872,rb_build(ARRAY[1781]))
     ON CONFLICT (uid) DO UPDATE SET user_list=rb_or(tb.user_list, rb_build(ARRAY[1782]))

    WITH upserts as (UPDATE flink_gp_bitmap AS tb set user_list=rb_or(tb.user_list, rb_build(ARRAY[1784, 1785])) where uid=55796873 returning *)
    INSERT INTO flink_gp_bitmap SELECT 55796873, rb_build(ARRAY[1781]) WHERE NOT EXISTS (SELECT 1 FROM upserts)
     */
    if (driverVersion.equals(PostgresValidator.CONNECTOR_VERSION_VALUE_95)) {
      s"""
         | INSERT INTO $tableName AS $tbAlias(${fieldNames.mkString(",")}) VALUES ($selectPlaceholder)
         | ON CONFLICT (${uniqueKeys.asScala.mkString(",")}) DO UPDATE SET $setPlaceHolder
       """.stripMargin
    } else {
      s"""
         | WITH upserts as (UPDATE $tableName AS $tbAlias SET $setPlaceHolder WHERE $conditionPlaceHolder RETURNING *)
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
      fieldSQLTypes(index) match {
        case Types.VARCHAR =>
          if (updateFieldIndex.contains(index)) {
            statement.setString(updateFieldIndex.indexOf(index) + 1, field.asInstanceOf[String])
          }
          if (conditionFieldIndex.contains(index)) {
            statement.setString(updateFieldIndex.length + conditionFieldIndex.indexOf(index) +1, field.asInstanceOf[String])
          }
          statement.setString(fieldSize+index+1, field.asInstanceOf[String])
        case Types.BIGINT =>
          if (updateFieldIndex.contains(index)) {
            statement.setLong(updateFieldIndex.indexOf(index) + 1, field.asInstanceOf[JLong])
          }
          if (conditionFieldIndex.contains(index)) {
            statement.setLong(updateFieldIndex.length + conditionFieldIndex.indexOf(index) +1, field.asInstanceOf[JLong])
          }
          statement.setLong(fieldSize + index + 1, field.asInstanceOf[JLong])
        case Types.DECIMAL =>
          if (updateFieldIndex.contains(index)) {
            statement.setBigDecimal(updateFieldIndex.indexOf(index) + 1, field.asInstanceOf[JBigDecimal])
          }
          if (conditionFieldIndex.contains(index)) {
            statement.setBigDecimal(updateFieldIndex.length + conditionFieldIndex.indexOf(index) +1, field.asInstanceOf[JBigDecimal])
          }
          statement.setBigDecimal(fieldSize+index+1, field.asInstanceOf[JBigDecimal])
        case Types.INTEGER =>
          if (updateFieldIndex.contains(index)) {
            statement.setInt(updateFieldIndex.indexOf(index) + 1, field.asInstanceOf[JInteger])
          }
          if (conditionFieldIndex.contains(index)) {
            statement.setInt(updateFieldIndex.length + conditionFieldIndex.indexOf(index) +1, field.asInstanceOf[JInteger])
          }
          statement.setInt(fieldSize+index+1, field.asInstanceOf[JInteger])
        case Types.DOUBLE =>
          if (updateFieldIndex.contains(index)) {
            statement.setDouble(updateFieldIndex.indexOf(index) + 1, field.asInstanceOf[JDouble])
          }
          if (conditionFieldIndex.contains(index)) {
            statement.setDouble(updateFieldIndex.length + conditionFieldIndex.indexOf(index) +1, field.asInstanceOf[JDouble])
          }
          statement.setDouble(fieldSize+index+1, field.asInstanceOf[JDouble])
        case Types.BOOLEAN =>
          if (updateFieldIndex.contains(index)) {
            statement.setBoolean(updateFieldIndex.indexOf(index) + 1, field.asInstanceOf[JBool])
          }
          if (conditionFieldIndex.contains(index)) {
            statement.setBoolean(updateFieldIndex.length + conditionFieldIndex.indexOf(index) +1, field.asInstanceOf[JBool])
          }
          statement.setBoolean(fieldSize+index+1, field.asInstanceOf[JBool])
        case Types.FLOAT =>
          if (updateFieldIndex.contains(index)) {
            statement.setFloat(updateFieldIndex.indexOf(index) + 1, field.asInstanceOf[JFloat])
          }
          if (conditionFieldIndex.contains(index)) {
            statement.setFloat(updateFieldIndex.length + conditionFieldIndex.indexOf(index) +1, field.asInstanceOf[JFloat])
          }
          statement.setFloat(fieldSize+index+1, field.asInstanceOf[JFloat])
        case Types.SMALLINT =>
          if (updateFieldIndex.contains(index)) {
            statement.setShort(updateFieldIndex.indexOf(index) + 1, field.asInstanceOf[JShort])
          }
          if (conditionFieldIndex.contains(index)) {
            statement.setShort(updateFieldIndex.length + conditionFieldIndex.indexOf(index) +1, field.asInstanceOf[JShort])
          }
          statement.setShort(fieldSize+index+1, field.asInstanceOf[JShort])
        case Types.TINYINT =>
          if (updateFieldIndex.contains(index)) {
            statement.setByte(updateFieldIndex.indexOf(index) + 1, field.asInstanceOf[JByte])
          }
          if (conditionFieldIndex.contains(index)) {
            statement.setByte(updateFieldIndex.length + conditionFieldIndex.indexOf(index) +1, field.asInstanceOf[JByte])
          }
          statement.setByte(fieldSize+index+1, field.asInstanceOf[JByte])
        case Types.ARRAY =>
          // TODO: convert to pgArray
          if (updateFieldIndex.contains(index)) {
            statement.setArray(updateFieldIndex.indexOf(index) + 1, field.asInstanceOf[JArray])
          }
          if (conditionFieldIndex.contains(index)) {
            statement.setArray(updateFieldIndex.length + conditionFieldIndex.indexOf(index) +1, field.asInstanceOf[JArray])
          }
          statement.setArray(fieldSize+index+1, field.asInstanceOf[JArray])
        case Types.DATE =>
          if (updateFieldIndex.contains(index)) {
            statement.setDate(updateFieldIndex.indexOf(index) + 1, field.asInstanceOf[JDate])
          }
          if (conditionFieldIndex.contains(index)) {
            statement.setDate(updateFieldIndex.length + conditionFieldIndex.indexOf(index) +1, field.asInstanceOf[JDate])
          }
          statement.setDate(fieldSize+index+1, field.asInstanceOf[JDate])
        case Types.TIME =>
          if (updateFieldIndex.contains(index)) {
            statement.setTime(updateFieldIndex.indexOf(index) + 1, field.asInstanceOf[JTime])
          }
          if (conditionFieldIndex.contains(index)) {
            statement.setTime(updateFieldIndex.length + conditionFieldIndex.indexOf(index) +1, field.asInstanceOf[JTime])
          }
          statement.setTime(fieldSize+index+1, field.asInstanceOf[JTime])
        case Types.TIMESTAMP =>
          if (updateFieldIndex.contains(index)) {
            statement.setTimestamp(updateFieldIndex.indexOf(index) + 1, field.asInstanceOf[JTimestamp])
          }
          if (conditionFieldIndex.contains(index)) {
            statement.setTimestamp(updateFieldIndex.length + conditionFieldIndex.indexOf(index) +1, field.asInstanceOf[JTimestamp])
          }
          statement.setTimestamp(fieldSize+index+1, field.asInstanceOf[JTimestamp])
        case Types.BINARY =>
          // convert to roaringBitMap
          val bytes = field.asInstanceOf[Array[Byte]]
          val ints = BytesUtil.bytesToInts(bytes).toSet.asJava.toArray()
          val array = dbConn.createArrayOf("int", ints)
          if (updateFieldIndex.contains(index)) {
            statement.setArray(updateFieldIndex.indexOf(index) + 1, array)
          }

          statement.setArray(fieldSize+index+1, array)
        case _ =>
          throw new IllegalArgumentException(s"column type: ${JDBCTypeUtil.getTypeName(fieldSQLTypes(index))} was not support so far...")

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
      fieldSQLTypes(index) match {
        case Types.VARCHAR =>
          if (updateFieldIndex.contains(index)) {
            statement.setString(fieldSize + updateFieldIndex.indexOf(index) + 1, field.asInstanceOf[String])
          }
          statement.setString(index + 1, field.asInstanceOf[String])
        case Types.BIGINT =>
          if (updateFieldIndex.contains(index)) {
            statement.setLong(fieldSize + updateFieldIndex.indexOf(index) + 1, field.asInstanceOf[JLong])
          }
          statement.setLong(index + 1, field.asInstanceOf[JLong])
        case Types.DECIMAL =>
          if (updateFieldIndex.contains(index)) {
            statement.setBigDecimal(fieldSize + updateFieldIndex.indexOf(index) + 1, field.asInstanceOf[JBigDecimal])
          }
          statement.setBigDecimal(index + 1, field.asInstanceOf[JBigDecimal])
        case Types.INTEGER =>
          if (updateFieldIndex.contains(index)) {
            statement.setInt(fieldSize + updateFieldIndex.indexOf(index) + 1, field.asInstanceOf[JInteger])
          }
          statement.setInt(index + 1, field.asInstanceOf[JInteger])
        case Types.DOUBLE =>
          if (updateFieldIndex.contains(index)) {
            statement.setDouble(fieldSize + updateFieldIndex.indexOf(index) + 1, field.asInstanceOf[JDouble])
          }
          statement.setDouble(index + 1, field.asInstanceOf[JDouble])
        case Types.BOOLEAN =>
          if (updateFieldIndex.contains(index)) {
            statement.setBoolean(fieldSize + updateFieldIndex.indexOf(index) + 1, field.asInstanceOf[JBool])
          }
          statement.setBoolean(index+1, field.asInstanceOf[JBool])
        case Types.FLOAT =>
          if (updateFieldIndex.contains(index)) {
            statement.setFloat(fieldSize + updateFieldIndex.indexOf(index) + 1, field.asInstanceOf[JFloat])
          }
          statement.setFloat(index+1, field.asInstanceOf[JFloat])
        case Types.SMALLINT =>
          if (updateFieldIndex.contains(index)) {
            statement.setShort(fieldSize + updateFieldIndex.indexOf(index) + 1, field.asInstanceOf[JShort])
          }
          statement.setShort(index+1, field.asInstanceOf[JShort])
        case Types.TINYINT =>
          if (updateFieldIndex.contains(index)) {
            statement.setByte(fieldSize + updateFieldIndex.indexOf(index) + 1, field.asInstanceOf[JByte])
          }
          statement.setByte(index+1, field.asInstanceOf[JByte])
        case Types.ARRAY =>
          if (updateFieldIndex.contains(index)) {
            statement.setArray(fieldSize + updateFieldIndex.indexOf(index) + 1, field.asInstanceOf[JArray])
          }
          statement.setArray(index+1, field.asInstanceOf[JArray])
        case Types.DATE =>
          if (updateFieldIndex.contains(index)) {
            statement.setDate(fieldSize + updateFieldIndex.indexOf(index) + 1, field.asInstanceOf[JDate])
          }
          statement.setDate(index+1, field.asInstanceOf[JDate])
        case Types.TIME =>
          if (updateFieldIndex.contains(index)) {
            statement.setTime(fieldSize + updateFieldIndex.indexOf(index) + 1, field.asInstanceOf[JTime])
          }
          statement.setTime(index+1, field.asInstanceOf[JTime])
        case Types.TIMESTAMP =>
          if (updateFieldIndex.contains(index)) {
            statement.setTimestamp(fieldSize + updateFieldIndex.indexOf(index) + 1, field.asInstanceOf[JTimestamp])
          }
          statement.setTimestamp(index+1, field.asInstanceOf[JTimestamp])
        case Types.BINARY =>
          // convert to RoaringBitMap
          val bytes = field.asInstanceOf[Array[Byte]]
          val ints = BytesUtil.bytesToInts(bytes).toSet.asJava.toArray()
          val array = dbConn.createArrayOf("int", ints)
          if (updateFieldIndex.contains(index)) {
            statement.setArray(fieldSize + updateFieldIndex.indexOf(index) + 1, array)
          }
          statement.setArray(index+1, array)
        case _ =>
          throw new IllegalArgumentException(s"column type: ${JDBCTypeUtil.getTypeName(fieldSQLTypes(index))} was not support so far...")
      }
    }
  }

}
