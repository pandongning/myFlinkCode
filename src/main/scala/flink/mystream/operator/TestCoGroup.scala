package flink.mystream.operator

import java.{lang, util}

import org.apache.flink.api.common.functions.CoGroupFunction
import org.apache.flink.streaming.api.scala.{DataStream, StreamExecutionEnvironment}
import org.apache.flink.api.scala._
import org.apache.flink.streaming.api.windowing.assigners.ProcessingTimeSessionWindows
import org.apache.flink.streaming.api.windowing.time.Time
import org.apache.flink.streaming.api.windowing.triggers.CountTrigger
import org.apache.flink.util.Collector

object TestCoGroup {
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

    stream1.coGroup(stream2)
      .where((tuple: (String, String)) => (tuple._1))
      .equalTo((tuple: (String, String)) => (tuple._1))
      .window(ProcessingTimeSessionWindows.withGap(Time.seconds(10)))
      .trigger(CountTrigger.of(5))
      .apply(new CoGroupFunction[(String, String), (String, String), String] {
        override def coGroup(first: lang.Iterable[(String, String)], second: lang.Iterable[(String, String)], out: Collector[String]): Unit = {
          val buffer: StringBuffer = new StringBuffer()
          val firstValue: util.Iterator[(String, String)] = first.iterator()
          while (firstValue.hasNext) {
            buffer.append(firstValue.next())
          }

          val secondValue: util.Iterator[(String, String)] = second.iterator()
          while (secondValue.hasNext) {
            buffer.append(secondValue.next())
          }

          out.collect(buffer.toString)
        }
      }).print()

    environment.execute("TestCoGroup")

  }
}
