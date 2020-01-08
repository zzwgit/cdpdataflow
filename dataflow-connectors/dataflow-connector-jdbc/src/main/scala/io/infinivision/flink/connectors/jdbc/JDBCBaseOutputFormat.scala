package io.infinivision.flink.connectors.jdbc

import java.lang.{Boolean => JBool}
import java.net.{ConnectException, InetAddress, SocketTimeoutException}
import java.sql.{Connection, DriverManager, PreparedStatement, SQLException}
import java.util.concurrent.atomic.{AtomicBoolean, AtomicInteger}
import java.util.concurrent.{ExecutorService, Executors, TimeUnit}

import org.apache.flink.api.common.io.RichOutputFormat
import org.apache.flink.api.java.tuple.{Tuple2 => JTuple2}
import org.apache.flink.configuration.Configuration
import org.apache.flink.table.util.Logging
import org.apache.flink.types.Row
import org.apache.http.conn.ConnectTimeoutException

import scala.concurrent._
import scala.concurrent.Future
import scala.util.{Failure, Success, Try}

abstract class JDBCBaseOutputFormat(
                                     private val userName: String,
                                     private val password: String,
                                     private val driverName: String,
                                     private val driverVersion: String,
                                     private var dbURL: String,
                                     private val tableName: String,
                                     private val fieldNames: Array[String],
                                     private val fieldSQLTypes: Array[Int],
                                     private val servers: Option[Array[String]],
                                     private val asyncFlush: Boolean
                                   ) extends RichOutputFormat[JTuple2[JBool, Row]]
  with Logging {

  protected var dbConn: Connection = _
  protected var statement: PreparedStatement = _
  private var batchCount: Int = 0
  protected var batchInterval: Int = 5000
  private var lastFlushTime: Long = 0
  private var sql: String = _
  @transient private var taskExecutor: ExecutorService = _
  @transient implicit private var ec: ExecutionContext = _
  @volatile private var hasError: Boolean = false
  private var flushException: java.lang.Throwable = _
  private val pendingFlush = new AtomicInteger(0)
  private var fillBatchMoreThanOneSecond: Boolean = false
  private var lastLoggingFlushTime: Long = 0
  private var taskNum: Int = 0

  def this(userName: String,
           password: String,
           driverName: String,
           driverVersion: String,
           dbURL: String,
           tableName: String,
           fieldNames: Array[String],
           fieldSQLTypes: Array[Int],
           asyncFlush: Boolean) {
    this(userName,
      password,
      driverName,
      driverVersion,
      dbURL,
      tableName,
      fieldNames,
      fieldSQLTypes,
      Option.empty,
      asyncFlush)
  }

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
      Option.empty,
      false)
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
    this.taskNum = taskNumber
    if (asyncFlush) {
      LOG.info("this jdbc output mode is async flush...")
      taskExecutor = Executors.newFixedThreadPool(1)
      ec = ExecutionContext.fromExecutor(taskExecutor)
    }
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
      sql = prepareSql
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
        flush()
      }
    } else {
      // do nothing so far
    }

  }

  private def logDebugFlush(stmt: PreparedStatement): Unit = {
    val curFlushTime = System.currentTimeMillis()
    flushBatch(stmt)
    val flushCost = System.currentTimeMillis() - curFlushTime
    val batchCostTime = curFlushTime - lastFlushTime
    if (batchCostTime > 1000) {
      fillBatchMoreThanOneSecond = true
    }
    lastFlushTime = curFlushTime
    if (lastLoggingFlushTime == 0) {
      lastLoggingFlushTime = curFlushTime // log first write
    }
    val elapsedTime = curFlushTime - lastLoggingFlushTime
    if (elapsedTime > 10 * 60 * 1000) { // log every ten minutes...
      lastLoggingFlushTime = curFlushTime
    } else if (elapsedTime <= 1000 || fillBatchMoreThanOneSecond) {
      // log duration: one second, if one batch more one second, log every batch
      LOG.debug("taskNum={} collect one batch cost {}ms, flush {} records to disk, flush cost {}ms ...", taskNum.toString, batchCostTime.toString, batchCount.toString, flushCost.toString)
      fillBatchMoreThanOneSecond = false
    }
  }

  private def flushBatch(stmt: PreparedStatement): Unit = {
    var i = 100
    while (true) {
      Try {
        stmt.executeBatch()
      } match {
        case Success(_) => return
        case Failure(ex) =>
          i -= 1
          // 注意 ex永远是ClickHouseException, 需要取 ex.getCause
          ex.getCause match {
            // timeout 重试 10 次
            case e@(_: Throwable) =>
              if (i <= 0) {
                LOG.error("connection timeout, retried 10 times still cannot recover, exit...")
                throw ex
              }
              Thread.sleep(1000 * 30)
            case _ => throw ex
          }
      }
    }
  }

  def flush(): Unit = {
    if (hasError) {
      throw new RuntimeException("previous flush error occurred...exit...", flushException)
    }
    // if too many pendingFlush, use syn mode
    val p = pendingFlush.get()
    if (asyncFlush && p == 0) {
      val old = statement
      statement = dbConn.prepareStatement(sql)
      pendingFlush.incrementAndGet()
      val future1 = Future {
        flushBatch(old)
        old.close() // remember close the statement
      }
      future1 onComplete {
        case Success(value) =>
          pendingFlush.decrementAndGet()
        case Failure(e) =>
          LOG.error("flush error occurred ", e)
          pendingFlush.decrementAndGet()
          flushException = e
          // notify
          hasError = true
      }
    } else {
      if (p >= 1) {
        LOG.debug("too many pending flush... {}", p)
      }
      logDebugFlush(statement)
    }
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

    if (taskExecutor != null) {
      while (pendingFlush.get() != 0) { // waiting last flush
        Thread.sleep(200)
      }
      if (hasError) {
        throw new RuntimeException("last flush error...")
      }
      taskExecutor.shutdown()
      taskExecutor.awaitTermination(java.lang.Long.MAX_VALUE, TimeUnit.NANOSECONDS)
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
