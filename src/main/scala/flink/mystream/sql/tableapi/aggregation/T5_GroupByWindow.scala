package flink.mystream.sql.tableapi.aggregation

import org.apache.flink.streaming.api.TimeCharacteristic
import org.apache.flink.streaming.api.functions.timestamps.BoundedOutOfOrdernessTimestampExtractor
import org.apache.flink.streaming.api.scala.{DataStream, StreamExecutionEnvironment}
import org.apache.flink.table.api.{EnvironmentSettings, Table, Tumble}
import org.apache.flink.table.api.scala.StreamTableEnvironment
import org.apache.flink.table.api.scala._
import org.apache.flink.api.scala._
import org.apache.flink.streaming.api.windowing.time.Time
import org.apache.flink.types.Row

object T5_GroupByWindow {
  def main(args: Array[String]): Unit = {

    val environment: StreamExecutionEnvironment = StreamExecutionEnvironment.getExecutionEnvironment
    environment.setStreamTimeCharacteristic(TimeCharacteristic.EventTime)
    environment.setParallelism(1)

    val environmentSettings: EnvironmentSettings = EnvironmentSettings.newInstance().inStreamingMode().useBlinkPlanner().build()
    val tableEnvironment: StreamTableEnvironment = StreamTableEnvironment.create(environment, environmentSettings)


    val sourceData: DataStream[String] = environment.socketTextStream("LocalOne", 7777)

    val rowDataStream: DataStream[(Long, String, String, Double)] = sourceData.map(
      line => {
        val strings: Array[String] = line.split(",")
        (strings(0).toLong, strings(1), strings(2), strings(3).toDouble)
      }
    )

    val rowWatermark: DataStream[(Long, String, String, Double)] = rowDataStream.assignTimestampsAndWatermarks(new BoundedOutOfOrdernessTimestampExtractor[(Long, String, String, Double)](Time.milliseconds(1)) {
      override def extractTimestamp(element: (Long, String, String, Double)): Long = {
        element._1
      }
    })

    //如果原始event里面没有时间戳字段，则需要自己添加一个eventTime字段。
    val table: Table = rowWatermark.toTable(tableEnvironment, 'atime.rowtime, 'uid, 'pid, 'money)
    table
      .window(Tumble.over(3.millis).on("atime").as("win"))
      .groupBy('uid, 'win)
      .select("uid, win.start, win.end, win.rowtime, money.sum as total")
      .toRetractStream[Row]
      .print()

    environment.execute()

  }
}
