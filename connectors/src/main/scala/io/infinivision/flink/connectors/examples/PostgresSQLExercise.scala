package io.infinivision.flink.connectors.examples

object PostgresSQLExercise {
  def main(args: Array[String]): Unit = {
    val tableSource = TableExerciseUtils.createPostgresTableSource()
  }
}
