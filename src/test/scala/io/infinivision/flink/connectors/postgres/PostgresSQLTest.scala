package io.infinivision.flink.connectors.postgres

import io.infinivision.flink.sql.SQLClientITCase
import org.apache.flink.streaming.api.TimeCharacteristic
import org.apache.flink.streaming.api.scala.StreamExecutionEnvironment
import org.apache.flink.table.api.TableEnvironment
import org.apache.flink.table.api.scala.StreamTableEnvironment
import org.apache.flink.table.client.config.Environment
import org.apache.flink.table.client.gateway.SessionContext
import org.apache.flink.table.runtime.utils.StreamTestSink
import org.junit.rules.{ExpectedException, TemporaryFolder}
import org.junit.{Before, Rule, Test}

class PostgresSQLTest extends SQLClientITCase {

  var env: StreamExecutionEnvironment = _
  var tEnv: StreamTableEnvironment = _
  val _tempFolder = new TemporaryFolder
  var enableObjectReuse = true
  // used for accurate exception information checking.
  val expectedException = ExpectedException.none()

  @Rule
  def thrown: ExpectedException = expectedException

  @Rule
  def tempFolder: TemporaryFolder = _tempFolder

  @Before
  override def before(): Unit = {
    super.before()
    StreamTestSink.clear()
    this.env = StreamExecutionEnvironment.getExecutionEnvironment
    env.setParallelism(4)
    this.env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime)
    if (enableObjectReuse) {
      this.env.getConfig.enableObjectReuse()
    }
    this.tEnv = TableEnvironment.getTableEnvironment(env)
  }

  @Test
  def testPostgresSQLQuery(): Unit = {
    val clusterClient = SQLClientITCase.miniClusterResource.getClusterClient
    val executor = createDefaultExecutor(clusterClient)
    val session = new SessionContext("test-session", new Environment)
    val ddl =
      """
        |CREATE TABLE adfeature (
        |  aid VARCHAR NOT NULL,
        |  advertiser_id VARCHAR NOT NULL,
        |  campaign_id VARCHAR NOT NULL,
        |  creative_id VARCHAR NOT NULL,
        |  creative_size VARCHAR NOT NULL,
        |  category_id VARCHAR NOT NULL,
        |  product_id VARCHAR NOT NULL,
        |  product_type VARCHAR NOT NULL,
        |  PRIMARY KEY(aid)
        |) WITH (
        |  type = 'postgres',
        |  connector.version = '9.4',
        |  username = 'postgres',
        |  password = '123456',
        |  tablename = 'adfeature',
        |  dburl = 'jdbc:postgresql://localhost:5432/postgres'
        |);
      """.stripMargin
    executor.createTable(session, ddl)

    val sql =
      """
        | SELECT
        |   aid, advertiser_id, category_id
        | FROM adfeature
      """.stripMargin

    val resultDesc = executor.executeQuery(session, sql)

    val result = retriveChangeLogResult(
      executor,
      session,
      resultDesc.getResultId
    )

    println(result.size)
  }

  @Test
  def testcsvSQLQuery(): Unit = {
    val clusterClient = SQLClientITCase.miniClusterResource.getClusterClient
    val executor = createDefaultExecutor(clusterClient)
    val session = new SessionContext("test-session", new Environment)
    val ddl =
      """
        |CREATE TABLE adfeature (
        |  aid VARCHAR NOT NULL,
        |  advertiser_id VARCHAR NOT NULL,
        |  campaign_id VARCHAR NOT NULL,
        |  creative_id VARCHAR NOT NULL,
        |  creative_size VARCHAR NOT NULL,
        |  category_id VARCHAR NOT NULL,
        |  product_id VARCHAR NOT NULL,
        |  product_type VARCHAR NOT NULL,
        |  PRIMARY KEY(aid)
        |) WITH (
        |  type = 'csv',
        |  firstLineAsHeader='true',
        |  path = 'file:///Users/hongtaozhang/Downloads/adFeature.csv'
        |);
      """.stripMargin
    executor.createTable(session, ddl)

    val sql =
      """
        | SELECT
        |   aid, advertiser_id, category_id
        | FROM adfeature
      """.stripMargin

    val resultDesc = executor.executeQuery(session, sql)

    val result = retriveChangeLogResult(
      executor,
      session,
      resultDesc.getResultId
    )

    println(result.size)
  }

  @Test
  def testcsvBigFileSQLQuery(): Unit = {
    val clusterClient = SQLClientITCase.miniClusterResource.getClusterClient
    val executor = createDefaultExecutor(clusterClient)
    val session = new SessionContext("test-session", new Environment)
    val ddl =
      """
        |CREATE TABLE adfeature (
        |  aid VARCHAR NOT NULL,
        |  uid VARCHAR NOT NULL,
        |  label VARCHAR NOT NULL
        |) WITH (
        |  type = 'csv',
        |  firstLineAsHeader='true',
        |  path = 'file:///Users/hongtaozhang/Downloads/train.csv'
        |);
      """.stripMargin
    executor.createTable(session, ddl)

    val sql =
      """
        | SELECT
        |   aid, uid, label
        | FROM adfeature
      """.stripMargin

    val resultDesc = executor.executeQuery(session, sql)

    val result = retriveChangeLogResult(
      executor,
      session,
      resultDesc.getResultId
    )

    println(result.size)
  }

  @Test
  def testTemporalTableJoin(): Unit = {
    val clusterClient = SQLClientITCase.miniClusterResource.getClusterClient
    val executor = createDefaultExecutor(clusterClient)
    val session = new SessionContext("test-session", new Environment)

    // create adfeature table(build side)
    val adfeatureDDL =
      """
        |CREATE TABLE adfeature (
        |  aid VARCHAR NOT NULL,
        |  advertiser_id VARCHAR NOT NULL,
        |  campaign_id VARCHAR NOT NULL,
        |  creative_id VARCHAR NOT NULL,
        |  creative_size VARCHAR NOT NULL,
        |  category_id VARCHAR NOT NULL,
        |  product_id VARCHAR NOT NULL,
        |  product_type VARCHAR NOT NULL,
        |  PRIMARY KEY(aid)
        |) WITH (
        |  type = 'postgres',
        |  connector.version = '9.4',
        |  username = 'postgres',
        |  password = '123456',
        |  tablename = 'adfeature',
        |  dburl = 'jdbc:postgresql://localhost:5432/postgres'
        |);
      """.stripMargin
    executor.createTable(session, adfeatureDDL)

    // create train table (probe side)
    val trainDDL =
      """
        |create table train (
        |  aid VARCHAR NOT NULL,
        |  uid VARCHAR NOT NULL,
        |  label VARCHAR NOT NULL
        |) with (
        |  type = 'csv',
        |  firstLineAsHeader='true',
        |  path = 'file:///Users/hongtaozhang/Downloads/train10.csv'
        |);
      """.stripMargin

    executor.createTable(session, trainDDL)

    val sql =
      """
        |SELECT
        |p.aid, p.uid, b.advertiser_id
        |FROM train AS p
        |INNER JOIN
        |adfeature FOR SYSTEM_TIME AS OF PROCTIME() AS b
        |ON p.aid = b.aid
      """.stripMargin

    val resultDesc = executor.executeQuery(session, sql)

    val result = retriveChangeLogResult(
      executor,
      session,
      resultDesc.getResultId
    )

    println(result.size)
  }


}

object PostgresSQLTest {

}