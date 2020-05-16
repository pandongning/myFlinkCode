package flink.mystream.operator

import org.apache.flink.streaming.api.scala.{DataStream, StreamExecutionEnvironment}
import org.apache.flink.api.scala._

object T2_Filter {
  def main(args: Array[String]): Unit = {
    val environment: StreamExecutionEnvironment = StreamExecutionEnvironment.getExecutionEnvironment

    val value: DataStream[Int] = environment.fromElements(1, 2, 3, 1)

    value.filter(i => i != 1).print()

    environment.execute()
  }
}
