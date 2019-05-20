package io.infinivision.flink.common.utils

import org.apache.flink.streaming.api.TimeCharacteristic
import org.apache.flink.streaming.api.scala.StreamExecutionEnvironment
import org.apache.flink.table.api.TableEnvironment
import org.apache.flink.table.api.scala.StreamTableEnvironment
import org.junit.{Before, Rule}
import org.junit.rules.{ExpectedException, TemporaryFolder}
class StreamingTestBase {

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
  def before(): Unit = {
    StreamTestSink.clear()
    this.env = StreamExecutionEnvironment.getExecutionEnvironment
    env.setParallelism(4)
    this.env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime)
    if (enableObjectReuse) {
      this.env.getConfig.enableObjectReuse()
    }
    this.tEnv = TableEnvironment.getTableEnvironment(env)
  }
}
