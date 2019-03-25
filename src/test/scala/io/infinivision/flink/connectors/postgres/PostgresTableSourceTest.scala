package io.infinivision.flink.connectors.postgres

import org.apache.flink.table.api.types.DataTypes
import org.apache.flink.table.runtime.utils.{StreamingTestBase, TestingAppendSink}
import org.apache.flink.table.util.TableProperties
import org.apache.flink.types.Row
import org.apache.flink.api.scala._
import org.apache.flink.table.api.scala._
import org.junit.Test


class PostgresTableSourceTest extends StreamingTestBase {

  private val tableProperties = new TableProperties
  tableProperties.property("username", "postgres")
  tableProperties.property("password", "123456")
  tableProperties.property("tablename", "visitor")
  tableProperties.property("dburl", "jdbc:postgresql://localhost:5432/postgres")

  // create postgres table source
  private val postgresTableSource = PostgresTableSource.builder()
    .tableProperties(tableProperties)
    .field("uid", DataTypes.INT)
    .field("sex", DataTypes.BOOLEAN)
    .field("age", DataTypes.INT)
    .build()

  @Test
  def testPostgresTableSource(): Unit = {
    // register table source
    tEnv.registerTableSource("pgsource", postgresTableSource)
    env.setParallelism(1)
    val sql =
      """
        | SELECT uid FROM pgsource
      """.stripMargin

    val result = tEnv.sqlQuery(sql).toAppendStream[Row]
    val sink = new TestingAppendSink
    result.addSink(sink)
    env.execute()

    // we just print out the table result
    println(sink.getAppendResults)
  }


}

object PostgresTableSourceTest {

}