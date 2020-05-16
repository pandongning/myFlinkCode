package flink.mystream.source

import org.apache.flink.streaming.api.scala.{DataStream, StreamExecutionEnvironment}
import org.apache.flink.streaming.api.scala._

object T2_ParallelFile {
  def main(args: Array[String]): Unit = {

    val environment: StreamExecutionEnvironment = StreamExecutionEnvironment.getExecutionEnvironment

    val lines: DataStream[String] = environment.readTextFile("D:\\input")

    val words: DataStream[String] = lines.flatMap(_.split(" ")).setParallelism(3)

    val tupleDataStream: DataStream[(String, Int)] = words.map((_, 1))

    val value: KeyedStream[(String, Int), String] = tupleDataStream.keyBy(_._1)

    val wordCount: DataStream[(String, Int)] = tupleDataStream.keyBy(_._1).sum(1)

    println("lines" + "\t" + lines.parallelism)
    println("words" + "\t" + words.parallelism)
    println("tupleDataStream"+"\t"+tupleDataStream.parallelism)

    wordCount.print()

    environment.execute()

  }
}
