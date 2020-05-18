package flink.mystream.sql.function

import org.apache.flink.table.api.scala._
import org.apache.flink.api.scala._
import org.apache.flink.types.Row
import flink.mystream.beans.SensorReading
import flink.mystream.utils.SensorReadingDataSource
import org.apache.flink.streaming.api.scala.{DataStream, StreamExecutionEnvironment}
import org.apache.flink.table.api.{Over, Table, Tumble}
import org.apache.flink.table.api.scala.StreamTableEnvironment

object T1_ComparisonFunctions {

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

    //    注意在写的时候，对于要匹配的模式是一个字符串，所以需要用单引号或者双引号引起来
    //    对于表里的列需要使用'id这种写法
    tableOne.select("*")
      //      .where('id.like("sensor%"))
      //      .where('id.in("sensor_1","sensor_2")) //in里面是具体的多个值
      //      .where('id.in(tableOne.select('id).where('id==="sensor_3"))) //in里面是一个子查询，所以in里面的值是可以动态变化的
      .where('temperature.between(1L, 3L))
      .toRetractStream[Row]
      .print()


    val table: Table = tableEnvironment.sqlQuery(
      s"""
         |select * from $tableOne
         |where id like 'sensor%'
         |""".stripMargin)

    table.toRetractStream[Row].print()

    environment.execute()
  }
}
