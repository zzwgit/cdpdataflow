package io.infinivision.flink.connectors.jdbc

import java.lang.{Boolean => JBool}
import java.sql.{Connection, DriverManager, PreparedStatement, SQLException}

import org.apache.flink.api.common.io.RichOutputFormat
import org.apache.flink.api.java.tuple.{Tuple2 => JTuple2}
import org.apache.flink.configuration.Configuration
import org.apache.flink.table.util.Logging
import org.apache.flink.types.Row

abstract class JDBCBaseOutputFormat(
    private val userName: String,
    private val password: String,
    private val driverName: String,
    private val driverVersion: String,
    private var dbURL: String,
    private val tableName: String,
    private val fieldNames: Array[String],
    private val fieldSQLTypes: Array[Int],
    private val servers: Option[Array[String]]
) extends RichOutputFormat[JTuple2[JBool, Row]]
    with Logging {

  protected var dbConn: Connection = _
  protected var statement: PreparedStatement = _
  private var batchCount: Int = 0
  protected var batchInterval: Int = 5000
  private var lastFlushTime: Long = 0


  def this(userName: String,
           password: String,
           driverName: String,
           driverVersion: String,
           dbURL: String,
           tableName: String,
           fieldNames: Array[String],
           fieldSQLTypes: Array[Int]) {
    this(userName,
         password,
         driverName,
         driverVersion,
         dbURL,
         tableName,
         fieldNames,
         fieldSQLTypes,
         Option.empty)
  }

  override def configure(parameters: Configuration): Unit = {}

  def batchInterval(batchInterval: Int): Unit = {
    this.batchInterval = batchInterval
  }

  private def establishConnection(): Unit = {
    Class.forName(driverName)
    if (userName == null) dbConn = DriverManager.getConnection(dbURL)
    else dbConn = DriverManager.getConnection(dbURL, userName, password)
  }

  override def open(taskNumber: Int, numTasks: Int): Unit = {
    servers.foreach { address =>
      val i = taskNumber % address.length
      val addr = address(i)
      dbURL = dbURL.replaceFirst("//[^/]+/", s"//$addr/")
      LOG.info(
        s"current taskNumber=$taskNumber task open dbURL=$dbURL numTasks=$numTasks")
    }
    establishConnection()
    if (dbConn.getMetaData
          .getTables(null, null, tableName, null)
          .next()) {
      // prepare the upsert sql
      val sql = prepareSql
      LOG.info(s"prepare sql $sql")
      statement = dbConn.prepareStatement(sql)
    } else {
      throw new SQLException(s"table $tableName doesn't exist")
    }
  }

  def prepareSql: String

  def updatePreparedStatement(row: Row): Unit

  override def writeRecord(record: JTuple2[JBool, Row]): Unit = {
    if (record.f0) {
      // write upsert record
      updatePreparedStatement(record.f1)
      statement.addBatch()
      batchCount += 1
      if (batchCount >= batchInterval) {
        val curFlushTime = System.currentTimeMillis()
        val elapsedTime = curFlushTime - lastFlushTime
        if (elapsedTime > 10*60*1000) { // log every ten minutes...
          lastFlushTime = curFlushTime
        } else if ( elapsedTime <= 1000) { // log duration: one second
          LOG.debug(s"flush $batchCount records to disk...")
        }
        flush()
      }
    } else {
      // do nothing so far
    }

  }

  def flush(): Unit = {
    statement.executeBatch()
    batchCount = 0
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
        LOG.error(s"JDBCOutputFormat could not be closed: ${ex.getMessage}")
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

}
