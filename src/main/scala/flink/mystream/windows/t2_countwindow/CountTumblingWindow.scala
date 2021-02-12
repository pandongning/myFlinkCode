package flink.mystream.windows.t2_countwindow

import org.apache.flink.api.scala._
import org.apache.flink.streaming.api.scala.{DataStream, StreamExecutionEnvironment}

object CountTumblingWindow {

  def main(args: Array[String]): Unit = {
    val environment: StreamExecutionEnvironment = StreamExecutionEnvironment.getExecutionEnvironment

    val line: DataStream[String] = environment.socketTextStream("LocalOne", 8888)

    line.flatMap(line => line.split(",")).map((_, 1)).keyBy(0)
      .countWindow(3)
      .sum(1).print()

    environment.execute()
  }

}
