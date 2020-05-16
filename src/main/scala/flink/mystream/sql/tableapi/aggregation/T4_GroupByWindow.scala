package flink.mystream.sql.tableapi.aggregation

import flink.mystream.beans.SensorReading
import org.apache.flink.streaming.api.TimeCharacteristic
import org.apache.flink.streaming.api.functions.timestamps.BoundedOutOfOrdernessTimestampExtractor
import org.apache.flink.streaming.api.scala.{DataStream, StreamExecutionEnvironment}
import org.apache.flink.table.api.{EnvironmentSettings, Table, Tumble}
import org.apache.flink.table.api.scala.StreamTableEnvironment
import org.apache.flink.table.api.scala._
import org.apache.flink.api.scala._
import org.apache.flink.streaming.api.windowing.time.Time
import org.apache.flink.types.Row

object T4_GroupByWindow {

  def main(args: Array[String]): Unit = {

    val environment: StreamExecutionEnvironment = StreamExecutionEnvironment.getExecutionEnvironment
    environment.setStreamTimeCharacteristic(TimeCharacteristic.EventTime)
    environment.setParallelism(1)

    val environmentSettings: EnvironmentSettings = EnvironmentSettings.newInstance().inStreamingMode().useBlinkPlanner().build()
    val tableEnvironment: StreamTableEnvironment = StreamTableEnvironment.create(environment, environmentSettings)


    val sourceData: DataStream[String] = environment.socketTextStream("LocalOne", 7777)

    val sensorReadingDataStream: DataStream[SensorReading] = sourceData.map(line => {
      val strings: Array[String] = line.split(",")
      SensorReading(strings(0).trim, strings(1).trim.toLong, strings(2).trim.toDouble)
    }).assignTimestampsAndWatermarks(new BoundedOutOfOrdernessTimestampExtractor[SensorReading](Time.milliseconds(1)) {
      override def extractTimestamp(element: SensorReading): Long = element.timestamp
    })


    // 此处得到结论，如果DataStream里面的event类型是scala的case class。
    // 则表里面字段的名字默认为class字段的名字。
    // 但是最好自己重新写一遍名字，因为我们需要指定eventTime字段。
    //  因为在DataStream里面已经指定了使用timestamp字段做为水印的时间戳，所以此处再次指定其为Table里面的eventTime
    val table: Table = sensorReadingDataStream
      .toTable(tableEnvironment, 'idOne, 'timestampOne.rowtime, 'temperatureOne)


    /**
     * 输入数据
     * sensor_1,1585018995390,1.0
     * sensor_2,1585018995391,1.0
     * sensor_1,1585018995392,1.0
     * sensor_2,1585018995393,1.0
     * sensor_1,1585018995394,1.0
     * sensor_1,1585018995395,1.0
     *
     * 下面是对于每个id，再按照时间窗口划分，然后再进行操做
     */
    table
      .window(Tumble.over(3.millis).on("timestampOne").as("w"))
      .groupBy('idOne, 'w)
      .select('idOne, 'w.start, 'w.end, 'temperatureOne.sum)
      .toRetractStream[Row]
      .print()

    environment.execute()
  }
}
