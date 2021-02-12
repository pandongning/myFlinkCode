package flink.mystream.windows.t4_watermaker

import flink.mystream.beans.SensorReading
import org.apache.flink.api.common.functions.{RichMapFunction, RuntimeContext}
import org.apache.flink.api.common.state.{StateTtlConfig, ValueState, ValueStateDescriptor}
import org.apache.flink.streaming.api.TimeCharacteristic
import org.apache.flink.streaming.api.scala.{DataStream, StreamExecutionEnvironment}
import org.apache.flink.api.scala._
import org.apache.flink.configuration.Configuration
import org.apache.flink.streaming.api.functions.timestamps.BoundedOutOfOrdernessTimestampExtractor
import org.apache.flink.streaming.api.windowing.time.Time

object T1 {

  def main(args: Array[String]): Unit = {

    val environment: StreamExecutionEnvironment = StreamExecutionEnvironment.getExecutionEnvironment

    //    设置使用那种时间
    environment.setStreamTimeCharacteristic(TimeCharacteristic.EventTime)
    //  设置多长的间隔分配watermaker。只有使用周期性分配策略的时候，才需要此处的配置
    environment.getConfig.setAutoWatermarkInterval(500)

    val dataStream: DataStream[String] = environment.socketTextStream("LocalOne", 8888)



    val value: DataStream[SensorReading] = dataStream.map(
      (line: String) => {
        val dataArray: Array[String] = line.split(",")
        SensorReading(dataArray(0).trim, dataArray(1).trim.toLong, dataArray(2).trim.toDouble)
      }
    )
      .assignTimestampsAndWatermarks(new BoundedOutOfOrdernessTimestampExtractor[SensorReading](Time.seconds(1)) {
        override def extractTimestamp(element: SensorReading): Long = {
          element.timestamp * 1000L
        }
      })
      .keyBy(_.id)
      .timeWindow(Time.seconds(5))
      .reduce((result, data) => {
        SensorReading(result.id, result.timestamp, result.temperature.min(data.temperature))
      })

    value.print()

    environment.execute()
  }
}
