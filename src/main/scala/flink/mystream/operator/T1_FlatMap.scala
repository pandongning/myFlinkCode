package flink.mystream.operator

import java.util

import org.apache.flink.api.common.functions.RichMapFunction
import org.apache.flink.streaming.api.scala.{DataStream, StreamExecutionEnvironment}
import org.apache.flink.api.scala._

import scala.collection.mutable

object T1_FlatMap {

  def main(args: Array[String]): Unit = {

    val environment: StreamExecutionEnvironment = StreamExecutionEnvironment.getExecutionEnvironment

    val aString: Array[Int] = Array(1, 2, 3)
    val bString: Array[Int] = Array(4, 5)


    val value: DataStream[Array[Int]] = environment.fromElements(aString, bString)

    //    将List打宽

    val value1: DataStream[Int] = value.flatMap((a: Array[Int]) => a)


    environment.execute()
  }

}
