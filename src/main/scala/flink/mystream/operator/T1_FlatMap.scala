package flink.mystream.operator

import org.apache.flink.api.common.functions.FlatMapFunction
import org.apache.flink.api.scala._
import org.apache.flink.streaming.api.scala.{DataStream, StreamExecutionEnvironment}
import org.apache.flink.util.Collector

object T1_FlatMap {

  def main(args: Array[String]): Unit = {

    val environment: StreamExecutionEnvironment = StreamExecutionEnvironment.getExecutionEnvironment

    val aString: Array[Int] = Array(1, 2, 3)
    val bString: Array[Int] = Array(4, 5)


    val value: DataStream[Array[Int]] = environment.fromElements(aString, bString)

    //    将List打宽

    val value1: DataStream[Int] = value.flatMap((a: Array[Int]) => a)

    val value2: DataStream[Int] = value.flatMap(new FlatMapFunction[Array[Int], Int] {
      override def flatMap(value: Array[Int], out: Collector[Int]): Unit = {
        value.foreach((ele: Int) => out.collect(ele))
      }
    })

    value2.print()

   environment.execute()
  }

}
