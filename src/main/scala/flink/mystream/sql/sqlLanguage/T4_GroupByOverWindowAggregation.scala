package flink.mystream.sql.sqlLanguage

import flink.mystream.beans.SensorReading
import flink.mystream.utils.SensorReadingDataSource
import org.apache.flink.streaming.api.scala.{DataStream, StreamExecutionEnvironment}
import org.apache.flink.table.api.{Over, Table, Tumble}
import org.apache.flink.table.api.scala.StreamTableEnvironment
import org.apache.flink.table.api.scala._
import org.apache.flink.api.scala._
import org.apache.flink.types.Row

object T4_GroupByOverWindowAggregation {

  def main(args: Array[String]): Unit = {

    val environment: StreamExecutionEnvironment = SensorReadingDataSource.environment
    val tableEnvironment: StreamTableEnvironment = SensorReadingDataSource.getTableEnvironment

    val sensorReadingDataStreamOne: DataStream[SensorReading] = SensorReadingDataSource.getDataSource("LocalOne", 7778)

    // 此处得到结论，如果DataStream里面的event类型是scala的case class。
    // 则表里面字段的名字默认为class字段的名字。
    // 但是最好自己重新写一遍名字，因为我们需要指定eventTime字段。
    //  因为在DataStream里面已经指定了使用timestamp字段做为水印的时间戳，所以此处再次指定其为Table里面的eventTime
    //    此处的表对象是一个inlined table。即没有注册的表
    val tableOne: Table = sensorReadingDataStreamOne
      .toTable(tableEnvironment, 'id, 'timestamp.rowtime, 'temperature)

    val table: Table = tableEnvironment.sqlQuery(
      s"""
         |select
         |SUM(temperature)
         |OVER (partition by id order by `timestamp` rows BETWEEN 2 PRECEDING AND CURRENT ROW)
         |from $tableOne
         |""".stripMargin)

    //    如果有许多的聚合操做，则应该使用下面的语法,
    val tableTwo: Table = tableEnvironment.sqlQuery(
      s"""
         |select SUM(temperature) OVER w,Max(temperature) OVER w
         |from $tableOne
         |WINDOW w AS (
         |  PARTITION BY id
         |  ORDER BY `timestamp`
         |  ROWS BETWEEN 2 PRECEDING AND CURRENT ROW)
         |""".stripMargin)

    table.toRetractStream[Row].print()

    environment.execute()
  }
}
