package flink.mystream.sql.function.userDefinedFunctions

import org.apache.flink.table.api.scala._
import org.apache.flink.api.scala._
import flink.mystream.beans.SensorReading
import flink.mystream.utils.SensorReadingDataSource
import org.apache.flink.api.common.typeinfo.{TypeInformation, Types}
import org.apache.flink.streaming.api.scala.{DataStream, StreamExecutionEnvironment}
import org.apache.flink.table.api.{Over, Table, Tumble}
import org.apache.flink.table.api.scala.StreamTableEnvironment
import org.apache.flink.table.functions.{ScalarFunction, TableFunction}
import org.apache.flink.types.Row

object T3_Test_AggregateFunction {

  def main(args: Array[String]): Unit = {

    val environment: StreamExecutionEnvironment = SensorReadingDataSource.environment
    val tableEnvironment: StreamTableEnvironment = SensorReadingDataSource.getTableEnvironment

    val sensorReadingDataStreamOne: DataStream[SensorReading] = SensorReadingDataSource.getDataSource("192.168.28.10", 7777)

    val value: DataStream[(String, Long, Int)] = sensorReadingDataStreamOne.map(x => {
      (x.id, x.timestamp.toLong, x.temperature.toInt)
    })

    // 此处得到结论，如果DataStream里面的event类型是scala的case class。
    // 则表里面字段的名字默认为class字段的名字。
    // 但是最好自己重新写一遍名字，因为我们需要指定eventTime字段。
    //  因为在DataStream里面已经指定了使用timestamp字段做为水印的时间戳，所以此处再次指定其为Table里面的eventTime
    //    此处的表对象是一个inlined table。即没有注册的表
    val tableOne: Table = value
      .toTable(tableEnvironment, 'id, 'level, 'temperature)

    tableEnvironment.registerFunction("myAgg", new T3_AggregateFunction)

    tableEnvironment.sqlQuery(
      s"""
         |select id,myAgg(level,temperature)
         |from $tableOne
         |group by id
         |""".stripMargin)
      .toRetractStream[Row]
      .print()

    environment.execute()
  }
}
