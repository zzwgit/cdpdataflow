package io.infinivision.flink.examples

import java.sql.{DriverManager, PreparedStatement}

import org.postgresql.jdbc.PgArray
import scala.collection.JavaConverters._

object PostgresBitmapExercise {
  val conn_str = "jdbc:postgresql://172.19.0.108:5432/db_dmx_stage"
  classOf[org.postgresql.Driver]

  def main(args: Array[String]) {
    var statement: PreparedStatement = null
    val conn = DriverManager.getConnection(conn_str, "gpadmin", "Welcome123@")
    try {
      // prepare statement
      statement = conn.prepareStatement("SELECT * FROM flink_gp_bitmap")

      // Execute Query
      val rs = statement.executeQuery()
      val columnCount = rs.getMetaData.getColumnCount
      while (rs.next) {
        for (index <- 1 to columnCount) {
          val obj = rs.getObject(index)
          println(s"column type: ${obj.getClass.getSimpleName}" )
          println(obj)
        }
      }

      println("===Execute Update===")
      val user_list = List(10, 11, 12, 13, 14)
      val pg_array = conn.createArrayOf("int", user_list.asJava.toArray)
      // execute update
      statement = conn.prepareStatement("update flink_gp_bitmap SET user_list=rb_build(?) where uid=?")
      statement.setArray(1, pg_array)
      statement.setInt(2, 1)
      statement.executeUpdate()

    } finally {
      if (statement != null) {
        statement.close()
      }
      conn.close()
    }
  }

}
