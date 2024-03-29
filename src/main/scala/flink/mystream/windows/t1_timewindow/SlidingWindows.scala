package flink.mystream.windows.t1_timewindow

import org.apache.flink.streaming.api.TimeCharacteristic
import org.apache.flink.streaming.api.scala.{DataStream, StreamExecutionEnvironment}
import org.apache.flink.streaming.api.windowing.time.Time
import org.apache.flink.api.scala._

object SlidingWindows {
  def main(args: Array[String]): Unit = {
    import org.apache.flink.streaming.api.functions.AssignerWithPeriodicWatermarks

    val environment: StreamExecutionEnvironment = StreamExecutionEnvironment.getExecutionEnvironment

    val text: DataStream[String] = environment.socketTextStream("LocalOne", 8888)

    environment.setStreamTimeCharacteristic(TimeCharacteristic.EventTime)


    text.flatMap((_: String).split(","))
      .map(((_: String), 1))
      .keyBy(0)
      .timeWindow(Time.seconds(5), Time.seconds(2))
      .allowedLateness(Time.seconds(2L))
      .sum(1)
      .print()
      .setParallelism(1)

    environment.execute("TumblingWindows")
  }
}
