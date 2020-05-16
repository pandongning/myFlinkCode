package flink.mystream.operator

import org.apache.flink.api.common.functions.RichMapFunction
import org.apache.flink.streaming.api.scala.{DataStream, StreamExecutionEnvironment}
import org.apache.flink.api.scala._

object T1_FlatMap {

  def main(args: Array[String]): Unit = {

    val environment: StreamExecutionEnvironment = StreamExecutionEnvironment.getExecutionEnvironment

    val lineDataStream: DataStream[String] = environment.fromElements("a b c", "d e")

    val wordDataStream: DataStream[String] = lineDataStream.flatMap(line => line.split(" "))

    lineDataStream.map(line => line)

    wordDataStream.print()

    environment.execute()
  }

}
