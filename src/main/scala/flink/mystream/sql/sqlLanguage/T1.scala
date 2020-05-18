package flink.mystream.sql.sqlLanguage

import flink.mystream.beans.SensorReading
import flink.mystream.utils.SensorReadingDataSource
import org.apache.flink.streaming.api.scala.{DataStream, StreamExecutionEnvironment}
import org.apache.flink.table.api.{Over, Table, Tumble}
import org.apache.flink.table.api.scala.StreamTableEnvironment
import org.apache.flink.table.api.scala._
import org.apache.flink.api.scala._
import org.apache.flink.types.Row

object T1 {

  def main(args: Array[String]): Unit = {
    val environment: StreamExecutionEnvironment = SensorReadingDataSource.environment
    val tableEnvironment: StreamTableEnvironment = SensorReadingDataSource.getTableEnvironment

    val sensorReadingDataStreamOne: DataStream[SensorReading] = SensorReadingDataSource.getDataSource("LocalOne", 7778)

    // 此处得到结论，如果DataStream里面的event类型是scala的case class。
    // 则表里面字段的名字默认为class字段的名字。
    // 但是最好自己重新写一遍名  d字，因为我们需要指定eventTime字段。
    //  因为在DataStream里面已经指定了使用timestamp字段做为水印的时间戳，所以此处再次指定其为Table里面的eventTime

    //    此处的表对象是一个inlined table。即没有注册的表
    val tableOne: Table = sensorReadingDataStreamOne
      .toTable(tableEnvironment, 'id, 'timestamp.rowtime, 'temperature)

    //    对于没有注册的表必须使用下面的语法进行查询
    tableEnvironment.sqlQuery(s"SELECT SUM(temperature) FROM $tableOne").toRetractStream[Row].print()


    val tableOneReg: String = tableOne.toString
    tableEnvironment.sqlQuery("SELECT SUM(temperature) FROM tableOneReg").toAppendStream[Row]
      .print()


    //    将流注册为一个临时的视图使用
    tableEnvironment.createTemporaryView("mtTable", sensorReadingDataStreamOne, 'id, 'timestamp.rowtime, 'temperature)
    tableEnvironment
      .sqlQuery("SELECT SUM(temperature) FROM mtTable").toRetractStream[Row]
      .print()
    environment.execute()
  }
}
