package io.infinivision.flink.udfs

import java.lang.{Integer => JInteger, Iterable => JIterable, Long => JLong}
import java.util.{List => JList}

import io.infinivision.flink.common.utils.BytesUtil
import org.apache.flink.table.api.dataview.ListView
import org.apache.flink.table.api.functions.AggregateFunction
import org.apache.flink.table.api.types.{DataType, DataTypes, RowType, TypeInfoWrappedDataType}
import org.apache.flink.table.dataformat.GenericRow
import org.apache.flink.table.typeutils.ListViewTypeInfo

import scala.collection.JavaConverters._
import org.apache.flink.table.api.scala._

import scala.collection.mutable

abstract class CollectListFunction[E](valueType: DataType) extends AggregateFunction[Array[Byte], GenericRow] {
  def accumulate(acc: GenericRow, value: E): Unit = {
    val list = acc.getField(0).asInstanceOf[ListView[E]]
    if (value != null) {
      list.add(value)
    }
  }

  def retract(acc: GenericRow, value: E): Unit = {
    if (null != value) {
      val list = acc.getField(0).asInstanceOf[ListView[E]]
      if (!list.remove(value)) {
        val retractList = acc.getField(1).asInstanceOf[ListView[E]]
        retractList.add(value)
      }
    }
  }

  def merge(acc: GenericRow, its: JIterable[GenericRow]): Unit = {
    val iter = its.iterator()
    while (iter.hasNext) {
      // merge the adding list
      val otherAcc = iter.next()
      val thisList = acc.getField(0).asInstanceOf[ListView[E]]
      val otherList = otherAcc.getField(0).asInstanceOf[ListView[E]]
      val accList = otherList.get
      if (accList != null) {
        val listIter = accList.iterator()
        while (listIter.hasNext) {
          thisList.add(listIter.next())
        }
      }

      // merge the retract list
      val otherRetractList = otherAcc.getField(1).asInstanceOf[ListView[E]]
      val thisRetractList = acc.getField(1).asInstanceOf[ListView[E]]
      val retractList = otherRetractList.get
      if (retractList != null) {
        val retractListIter = retractList.iterator()
        var buffer: JList[E] = null
        if (retractListIter.hasNext) {
          buffer = thisList.get.asInstanceOf[JList[E]]
        }
        var listChanged = false
        while (retractListIter.hasNext) {
          val element = retractListIter.next()
          if (buffer != null && buffer.remove(element)) {
            listChanged = true
          } else {
            thisRetractList.add(element)
          }
        }

        if (listChanged) {
          thisList.clear()
          thisList.addAll(buffer)
        }
      }
    }
  }

  override def createAccumulator(): GenericRow = {
    val acc = new GenericRow(2)
    // adding list
    acc.update(0, new ListView[E](valueType))
    // retractList
    acc.update(1, new ListView[E](valueType))
    acc
  }

  def resetAccumulator(acc: GenericRow): Unit = {
    val list = acc.getField(0).asInstanceOf[ListView[E]]
    val retractList = acc.getField(1).asInstanceOf[ListView[E]]
    list.clear()
    retractList.clear()
  }

  override def getAccumulatorType: DataType = {
    val fieldTypes: Array[DataType] = Array(
      new TypeInfoWrappedDataType(new ListViewTypeInfo(valueType)),
      new TypeInfoWrappedDataType(new ListViewTypeInfo(valueType))
    )

    val fieldNames = Array("addinglist", "retractList")
    new RowType(fieldTypes, fieldNames)
  }

  override def getResultType: DataType = {
    DataTypes.BYTE_ARRAY
  }

}

class LongCollectListFunction extends CollectListFunction[JLong](DataTypes.LONG) {
  override def getValue(acc: GenericRow): Array[Byte] = {
    val result = new mutable.ArrayBuffer[JLong]
    val iter = acc.getField(0).asInstanceOf[ListView[JLong]].get
    if (iter != null) {
      val iterator = iter.iterator()
      while (iterator.hasNext) {
        result += iterator.next()
      }
    }

    BytesUtil.longsToBytes(result.asJava)
  }
}

class IntCollectListFunction extends CollectListFunction[JInteger](DataTypes.INT) {
  override def getValue(acc: GenericRow): Array[Byte] = {
    val result = new mutable.ArrayBuffer[JInteger]
    val iter = acc.getField(0).asInstanceOf[ListView[JInteger]].get
    if (iter != null) {
      val iterator = iter.iterator()
      while (iterator.hasNext) {
        result += iterator.next()
      }
    }

    BytesUtil.intsToBytes(result.asJava)
  }
}

