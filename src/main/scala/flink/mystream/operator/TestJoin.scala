package flink.mystream.operator

import org.apache.flink.api.common.functions.JoinFunction
import org.apache.flink.streaming.api.scala.{DataStream, StreamExecutionEnvironment}
import org.apache.flink.api.scala._
import org.apache.flink.streaming.api.windowing.assigners.ProcessingTimeSessionWindows
import org.apache.flink.streaming.api.windowing.time.Time

object TestJoin {
  def main(args: Array[String]): Unit = {
    val environment: StreamExecutionEnvironment = StreamExecutionEnvironment.getExecutionEnvironment

    val stream1: DataStream[(String, String)] = environment.socketTextStream("LocalOne", 9998)
      .map((item: String) => {
        val strings: Array[String] = item.split(" ")
        println("stream1" + "_" + (strings(0), strings(1)))
        (strings(0), strings(1))
      })


    val stream2: DataStream[(String, String)] = environment.socketTextStream("LocalOne", 9997)
      .map((item: String) => {
        val strings: Array[String] = item.split(" ")
        println("stream2" + "_" + (strings(0), strings(1)))
        (strings(0), strings(1))
      })

    stream1.join(stream2)
      .where((tuple: (String, String)) => tuple._1)
      .equalTo((tuple: (String, String)) => tuple._1)
      .window(ProcessingTimeSessionWindows.withGap(Time.seconds(10)))
      .apply(new JoinFunction[(String, String), (String, String), String] {
        override def join(first: (String, String), second: (String, String)): String = {
          first._1 + "---" + second._2
        }
      }).print()

    environment.execute("TestJoin")

  }
}
