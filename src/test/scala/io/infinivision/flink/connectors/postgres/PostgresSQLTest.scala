package io.infinivision.flink.connectors.postgres

import io.infinivision.flink.sql.SQLClientITCase
import org.apache.flink.table.client.config.Environment
import org.apache.flink.table.client.gateway.SessionContext
import org.junit.Test

class PostgresSQLTest extends SQLClientITCase {


  @Test
  def testPostgresSQLQuery(): Unit = {
    log.info("test create table...")
    val clusterClient = SQLClientITCase.miniClusterResource.getClusterClient
    val executor = createDefaultExecutor(clusterClient)
    val session = new SessionContext("test-session", new Environment)
    val ddl =
      """
        |CREATE TABLE csv_adfeature (
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
        | FROM csv_adfeature
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