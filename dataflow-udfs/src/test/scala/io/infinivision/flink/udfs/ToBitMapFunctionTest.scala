package io.infinivision.flink.udfs

import io.infinivision.flink.common.utils.{StreamTestData, StreamingTestBase, TestingRetractSink}
import org.apache.flink.table.api.scala._
import org.apache.flink.api.scala._
import org.apache.flink.types.Row
import org.junit.Assert.assertEquals
import org.junit.Test

class ToBitMapFunctionTest extends StreamingTestBase{

  @Test
  def testIntBitMapFunction(): Unit = {
//    val sqlQuery =
//      """
//        | SELECT b, int_collect_list(a) FROM T1 GROUP BY b
//      """.stripMargin
//
//    tEnv.registerFunction("int_to_bitmap", new IntToBitMapFunction)
//    val t1 = StreamTestData.get3TupleDataStream(env)
//    tEnv.registerDataStream("T1", t1, 'a, 'b, 'c)
//
//    val sink = new TestingRetractSink
//    tEnv.sqlQuery(sqlQuery).toRetractStream[Row].addSink(sink)
//    env.execute()
//
//    val expected = List(
//      "1,[0, 0, 0, 1]",
//      "2,[0, 0, 0, 2, 0, 0, 0, 3]",
//      "3,[0, 0, 0, 4, 0, 0, 0, 5, 0, 0, 0, 6]",
//      "4,[0, 0, 0, 7, 0, 0, 0, 8, 0, 0, 0, 9, 0, 0, 0, 10]",
//      "5,[0, 0, 0, 11, 0, 0, 0, 12, 0, 0, 0, 13, 0, 0, 0, 14, 0, 0, 0, 15]",
//      "6,[0, 0, 0, 16, 0, 0, 0, 17, 0, 0, 0, 18, 0, 0, 0, 19, 0, 0, 0, 20, 0, 0, 0, 21]")
//
//    assertEquals(expected.sorted, sink.getRetractResults.sorted)
  }

  @Test
  def testLongBitMapFunction(): Unit = {
//    val sqlQuery =
//      """
//        | SELECT a, long_collect_list(b) FROM T1 GROUP BY a
//      """.stripMargin
//
//    tEnv.registerFunction("long_to_bitmap", new LongToBitMapFunction)
//    val t1 = StreamTestData.get3TupleDataStream(env)
//    tEnv.registerDataStream("T1", t1, 'a, 'b, 'c)
//
//    val sink = new TestingRetractSink
//    tEnv.sqlQuery(sqlQuery).toRetractStream[Row].addSink(sink)
//    env.execute()
//
//    val expected = List(
//      "1,[0, 0, 0, 0, 0, 0, 0, 1]",
//      "10,[0, 0, 0, 0, 0, 0, 0, 4]",
//      "11,[0, 0, 0, 0, 0, 0, 0, 5]",
//      "12,[0, 0, 0, 0, 0, 0, 0, 5]",
//      "13,[0, 0, 0, 0, 0, 0, 0, 5]",
//      "14,[0, 0, 0, 0, 0, 0, 0, 5]",
//      "15,[0, 0, 0, 0, 0, 0, 0, 5]",
//      "16,[0, 0, 0, 0, 0, 0, 0, 6]",
//      "17,[0, 0, 0, 0, 0, 0, 0, 6]",
//      "18,[0, 0, 0, 0, 0, 0, 0, 6]",
//      "19,[0, 0, 0, 0, 0, 0, 0, 6]",
//      "2,[0, 0, 0, 0, 0, 0, 0, 2]",
//      "20,[0, 0, 0, 0, 0, 0, 0, 6]",
//      "21,[0, 0, 0, 0, 0, 0, 0, 6]",
//      "3,[0, 0, 0, 0, 0, 0, 0, 2]",
//      "4,[0, 0, 0, 0, 0, 0, 0, 3]",
//      "5,[0, 0, 0, 0, 0, 0, 0, 3]",
//      "6,[0, 0, 0, 0, 0, 0, 0, 3]",
//      "7,[0, 0, 0, 0, 0, 0, 0, 4]",
//      "8,[0, 0, 0, 0, 0, 0, 0, 4]",
//      "9,[0, 0, 0, 0, 0, 0, 0, 4]")
//
//    assertEquals(expected.sorted, sink.getRetractResults.sorted)
  }
}
