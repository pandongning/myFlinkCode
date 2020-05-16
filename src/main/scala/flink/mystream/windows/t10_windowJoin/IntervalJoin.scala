package flink.mystream.windows.t10_windowJoin

import org.apache.flink.streaming.api.TimeCharacteristic
import org.apache.flink.streaming.api.functions.co.ProcessJoinFunction
import org.apache.flink.streaming.api.functions.timestamps.BoundedOutOfOrdernessTimestampExtractor
import org.apache.flink.streaming.api.scala.StreamExecutionEnvironment
import org.apache.flink.streaming.api.windowing.time.Time
import org.apache.flink.streaming.api.scala._
import org.apache.flink.util.Collector

object IntervalJoin {

  def main(args: Array[String]): Unit = {
    val environment: StreamExecutionEnvironment = StreamExecutionEnvironment.getExecutionEnvironment
    environment.setStreamTimeCharacteristic(TimeCharacteristic.EventTime)

    val orangeStream: KeyedStream[(String, Int, String), String] = environment
      .fromElements(("sensor_1", 1567759291, "orangeStream1"), ("sensor_3", 1567759298, "orangeStream2"), ("sensor_2", 1567759293, "orangeStream3"))
      .assignTimestampsAndWatermarks(new BoundedOutOfOrdernessTimestampExtractor[(String, Int, String)](Time.milliseconds(1)) {
        override def extractTimestamp(element: (String, Int, String)): Long = {
          element._2
        }
      })
      .keyBy(_._1)

    val greenStream: KeyedStream[(String, Int, String), String] = environment
      .fromElements(("sensor_1", 1567759292, "greenStream1"), ("sensor_2", 1567759293, "greenStream2"))
      .assignTimestampsAndWatermarks(new BoundedOutOfOrdernessTimestampExtractor[(String, Int, String)](Time.milliseconds(1)) {
        override def extractTimestamp(element: (String, Int, String)): Long = {
          element._2
        }
      }).keyBy(_._1)

    //    两个流里面key相同的元素被聚合在一起
    greenStream
      .intervalJoin(orangeStream)
      .between(Time.milliseconds(-2), Time.milliseconds(2))
      .process(new MyProcessJoinFunction)
      .print()

    environment.execute()

  }
}

class MyProcessJoinFunction extends ProcessJoinFunction[(String, Int, String), (String, Int, String), (String, String, String)] {
  override def processElement(left: (String, Int, String), right: (String, Int, String), ctx: ProcessJoinFunction[(String, Int, String), (String, Int, String), (String, String, String)]#Context, out: Collector[(String, String, String)]): Unit = {
    out.collect((left._1 + "$" + right._1, left._2 + "$" + right._2, left._3 + "$" + right._3))
  }
}
