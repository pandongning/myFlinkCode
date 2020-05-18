package flink.mystream.sql.function

import org.apache.flink.table.api.scala._
import org.apache.flink.api.scala._
import org.apache.flink.types.Row
import flink.mystream.beans.SensorReading
import flink.mystream.utils.SensorReadingDataSource
import org.apache.flink.streaming.api.scala.{DataStream, StreamExecutionEnvironment}
import org.apache.flink.table.api.{Over, Table, Tumble}
import org.apache.flink.table.api.scala.StreamTableEnvironment
import org.apache.flink.table.expressions.TimeIntervalUnit

object T5_Conditional_Functions {

  def main(args: Array[String]): Unit = {

    val environment: StreamExecutionEnvironment = SensorReadingDataSource.environment
    val tableEnvironment: StreamTableEnvironment = SensorReadingDataSource.getTableEnvironment

    val sensorReadingDataStreamOne: DataStream[SensorReading] = SensorReadingDataSource.getDataSource("LocalOne", 7777)

    // 此处得到结论，如果DataStream里面的event类型是scala的case class。
    // 则表里面字段的名字默认为class字段的名字。
    // 但是最好自己重新写一遍名字，因为我们需要指定eventTime字段。
    //  因为在DataStream里面已经指定了使用timestamp字段做为水印的时间戳，所以此处再次指定其为Table里面的eventTime
    //    此处的表对象是一个inlined table。即没有注册的表
    val tableOne: Table = sensorReadingDataStreamOne
      .toTable(tableEnvironment, 'id, 'timestamp.rowtime, 'temperature)

    tableEnvironment.sqlQuery(
      s"""
         |select id, if(id>'sensor_2','aa','bb')
         |from $tableOne
         |""".stripMargin)
      .toRetractStream[Row]
    //      .print()


    //    下面的case用于穷举所有id的可能情况
    tableEnvironment.sqlQuery(
      s"""
         |
         |select id as myid,
         |CASE id
         |WHEN 'sensor_1','sensor_2' THEN 'aa'
         |WHEN 'sensor_2','sensor_3' THEN 'bb'
         |ELSE 'cc'
         |END as myResult
         |
         |from $tableOne
         |""".stripMargin)
      .toRetractStream[Row]
    //      .print()

    tableEnvironment.sqlQuery(
      s"""
         |
         |select id,temperature,
         |CASE
         |WHEN temperature>3 and temperature<30 THEN 'high'
         |WHEN temperature>30 THEN 'highest'
         |ELSE 'low'
         |END as level
         |from $tableOne
         |
         |""".stripMargin)
      .toRetractStream[Row]
    //      .print()


    //    子查询居然可以直接这样写，太好了
    tableEnvironment.sqlQuery(
      s"""
         |
         |select id,level
         |from
         |(select id,temperature,
         |CASE
         |WHEN temperature>3 and temperature<30 THEN 'high'
         |WHEN temperature>30 THEN 'highest'
         |ELSE 'low'
         |END as level
         |from $tableOne) TemporaryTables
         |
         |""".stripMargin)
      .toRetractStream[Row]
    //      .print()


    tableEnvironment.sqlQuery(
      s"""
         |select id,IS_DIGIT(temperature)
         |from $tableOne
         |""".stripMargin)
      .toAppendStream[Row]
      .print()


    environment.execute()
  }
}
