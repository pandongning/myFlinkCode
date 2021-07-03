package flink.mystream.sql.tableapi.joins

import flink.mystream.beans.SensorReading
import flink.mystream.utils.SensorReadingDataSource
import org.apache.flink.streaming.api.scala.{DataStream, StreamExecutionEnvironment}
import org.apache.flink.table.api.{Over, Table, Tumble}
import org.apache.flink.table.api.scala.StreamTableEnvironment
import org.apache.flink.table.api.scala._
import org.apache.flink.api.scala._
import org.apache.flink.streaming.api.functions.timestamps.BoundedOutOfOrdernessTimestampExtractor
import org.apache.flink.streaming.api.windowing.time.Time
import org.apache.flink.types.Row

object T9_Time_Windowed_Join {

  def main(args: Array[String]): Unit = {
    val environment: StreamExecutionEnvironment = SensorReadingDataSource.environment
    val tableEnvironment: StreamTableEnvironment = SensorReadingDataSource.getTableEnvironment

    val sensorReadingDataStream: DataStream[SensorReading] = SensorReadingDataSource.getDataSource("LocalOne", 7777)

    // 此处得到结论，如果DataStream里面的event类型是scala的case class。
    // 则表里面字段的名字默认为class字段的名字。
    // 但是最好自己重新写一遍名字，因为我们需要指定eventTime字段。
    //  因为在DataStream里面已经指定了使用timestamp字段做为水印的时间戳，所以此处再次指定其为Table里面的eventTime
    val tableOne: Table = sensorReadingDataStream
      .toTable(tableEnvironment, 'id, 'timestamp.rowtime, 'temperature)

    val soureTow: DataStream[String] = environment.socketTextStream("LocalOne", 7778)
    val soureTowWithWaterMaker: DataStream[SensorReading] = soureTow.map(line => {
      val strings: Array[String] = line.split(",")
      SensorReading(strings(0).trim, strings(1).trim.toLong, strings(2).trim.toDouble)
    }).assignTimestampsAndWatermarks(new BoundedOutOfOrdernessTimestampExtractor[SensorReading](Time.milliseconds(1)) {
      override def extractTimestamp(element: SensorReading): Long = element.timestamp
    })

    val tableTwo: Table = soureTowWithWaterMaker.toTable(tableEnvironment, 'idTwo, 'timestampTwo.rowtime, 'temperatureTwo)

    //    必须给两个流上设置eventTime字段
    //但是最后输出的时候只能选择一个流里面的eventTime字段输出
    //不能将两个流里面的eventTime字段都输出

    tableOne.
      leftOuterJoin(tableTwo)
      .where('id === 'idTwo && 'timestamp >= 'timestampTwo - 3.seconds && 'timestamp < 'timestampTwo + 2.seconds)
      .select('id, 'idTwo, 'timestamp, 'timestampTwo, 'temperature, 'temperatureTwo)
      .toRetractStream[Row]
      .print()

    environment.execute()
  }
}
