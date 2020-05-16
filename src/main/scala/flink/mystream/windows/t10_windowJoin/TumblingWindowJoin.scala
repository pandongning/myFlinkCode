package flink.mystream.windows.t10_windowJoin

import org.apache.flink.api.common.functions.JoinFunction
import org.apache.flink.streaming.api.TimeCharacteristic
import org.apache.flink.streaming.api.functions.timestamps.BoundedOutOfOrdernessTimestampExtractor
import org.apache.flink.streaming.api.scala.{StreamExecutionEnvironment, _}
import org.apache.flink.streaming.api.windowing.assigners.TumblingEventTimeWindows
import org.apache.flink.streaming.api.windowing.time.Time

object TumblingWindowJoin {

  def main(args: Array[String]): Unit = {
    val environment: StreamExecutionEnvironment = StreamExecutionEnvironment.getExecutionEnvironment
    environment.setStreamTimeCharacteristic(TimeCharacteristic.EventTime)

    val orangeStream: DataStream[(String, Int, String)] = environment
      .fromElements(("sensor_1", 1567759290, "orangeStream"), ("sensor_1", 1567759291, "orangeStream"), ("sensor_2", 1567759294, "orangeStream"))
      .assignTimestampsAndWatermarks(new BoundedOutOfOrdernessTimestampExtractor[(String, Int, String)](Time.milliseconds(1)) {
        override def extractTimestamp(element: (String, Int, String)): Long = {
          element._2
        }
      })

    val greenStream: DataStream[(String, Int, String)] = environment
      .fromElements(("sensor_1", 1567759292, "greenStream1"), ("sensor_1", 1567759293, "greenStream2"))
      .assignTimestampsAndWatermarks(new BoundedOutOfOrdernessTimestampExtractor[(String, Int, String)](Time.milliseconds(1)) {
        override def extractTimestamp(element: (String, Int, String)): Long = {
          element._2
        }
      })

    orangeStream.join(greenStream)
      .where(_._1) //指定orangeStream流里面的字段
      .equalTo(_._1) //指定greenStream流里面的字段
      .window(TumblingEventTimeWindows.of(Time.milliseconds(5L)))
      .apply(new MyJoinFunction())
      .print()

    environment.execute()

  }
}

class MyJoinFunction extends JoinFunction[(String, Int, String), (String, Int, String), (String, String, String)] {
  //  此处输入的是两个stream里面.key相同的对
  override def join(first: (String, Int, String), second: (String, Int, String)): (String, String, String) = {
    (first._1 + "$" + second._1, first._2 + "$" + second._2, first._3 + "$" + second._3)
  }
}
