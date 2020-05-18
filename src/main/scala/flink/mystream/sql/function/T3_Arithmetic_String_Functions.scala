package flink.mystream.sql.function


import org.apache.flink.table.api.scala._
import org.apache.flink.api.scala._
import org.apache.flink.types.Row
import flink.mystream.beans.SensorReading
import flink.mystream.utils.SensorReadingDataSource
import org.apache.flink.streaming.api.scala.{DataStream, StreamExecutionEnvironment}
import org.apache.flink.table.api.{Over, Table, Tumble}
import org.apache.flink.table.api.scala.StreamTableEnvironment

object T3_Arithmetic_String_Functions {

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

    tableEnvironment
      .sqlQuery(
        s"""
           |select id,temperature+3 from $tableOne
           |where id > 'sensor_1'
           |""".stripMargin)
      .toRetractStream[Row]
    //      .print()

//    转换为case class
    val value: DataStream[(Boolean, SensorReading)] = tableOne.select("*")
      .where('id === "sensor_1")
      .toRetractStream[SensorReading]



    tableOne
      .select('id, 'temperature.sqrt(), rand(23), 'temperature + rand(23))
      .where('id === "sensor_1")
      .toRetractStream[Row]
    //      .print()

    tableOne
      //      .select('id.upperCase(), 'timestamp, 'temperature)
      //      .select('id.trim(removeLeading = true,removeTrailing = true," "))
      //      .select('id.repeat(3), 'timestamp, 'temperature)
      //      .select(concat('id, "pdn"))
      .select(concat_ws("$", 'id, "pdn", " ", "aa"))
      .toRetractStream[Row]

      .print()


    val table: Table = tableEnvironment.sqlQuery(
      s"""
         |select repeat(id,2)
         |from $tableOne
         |""".stripMargin)
    table.toRetractStream[Row]
    //      .print()


    environment.execute()

  }
}
