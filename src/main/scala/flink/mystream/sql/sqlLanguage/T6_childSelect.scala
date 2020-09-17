package flink.mystream.sql.sqlLanguage

import org.apache.flink.table.api.scala._
import org.apache.flink.api.scala._
import org.apache.flink.types.Row
import flink.mystream.beans.SensorReading
import flink.mystream.utils.SensorReadingDataSource
import org.apache.flink.streaming.api.scala.{DataStream, StreamExecutionEnvironment}
import org.apache.flink.table.api.{Over, Table, Tumble}
import org.apache.flink.table.api.scala.StreamTableEnvironment
import org.apache.flink.table.expressions.TimeIntervalUnit

object T6_childSelect {
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
          .print()


    environment.execute()
  }
}
