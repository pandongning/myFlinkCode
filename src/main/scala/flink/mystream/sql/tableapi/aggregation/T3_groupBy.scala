package flink.mystream.sql.tableapi.aggregation

import flink.mystream.beans.SensorReading
import org.apache.flink.streaming.api.TimeCharacteristic
import org.apache.flink.streaming.api.functions.timestamps.BoundedOutOfOrdernessTimestampExtractor
import org.apache.flink.streaming.api.scala.{DataStream, StreamExecutionEnvironment}
import org.apache.flink.table.api.{EnvironmentSettings, Table}
import org.apache.flink.table.api.scala.StreamTableEnvironment
import org.apache.flink.table.api.scala._
import org.apache.flink.api.scala._
import org.apache.flink.streaming.api.windowing.time.Time
import org.apache.flink.types.Row

object T3_groupBy {

  def main(args: Array[String]): Unit = {

    val environment: StreamExecutionEnvironment = StreamExecutionEnvironment.getExecutionEnvironment
    environment.setStreamTimeCharacteristic(TimeCharacteristic.EventTime)

    val environmentSettings: EnvironmentSettings = EnvironmentSettings.newInstance().inStreamingMode().useBlinkPlanner().build()
    val tableEnvironment: StreamTableEnvironment = StreamTableEnvironment.create(environment, environmentSettings)

    val path: String = getClass.getClassLoader.getResource("sensor.txt").getPath
    val sourceData: DataStream[String] = environment.readTextFile(path)

    val sensorReadingDataStream: DataStream[SensorReading] = sourceData.map(line => {
      val strings: Array[String] = line.split(",")
      SensorReading(strings(0).trim, strings(1).trim.toLong, strings(2).trim.toDouble)
    }).assignTimestampsAndWatermarks(new BoundedOutOfOrdernessTimestampExtractor[SensorReading](Time.milliseconds(1)) {
      override def extractTimestamp(element: SensorReading): Long = element.timestamp
    })


    // 此处得到结论，如果DataStream里面的event类型是scala的case class。
    // 则表里面字段的名字默认为class字段的名字。
    // 但是最好自己重新写一遍名字，因为我们需要指定eventTime字段。
    val table: Table = sensorReadingDataStream
      .toTable(tableEnvironment, 'id, 'timestamp.rowtime, 'temperature)

    table
      .groupBy('id)
      .select('id, 'temperature.sum as 'sumTemperature)
      .toRetractStream[Row]
      .print()

    environment.execute()
  }
}
