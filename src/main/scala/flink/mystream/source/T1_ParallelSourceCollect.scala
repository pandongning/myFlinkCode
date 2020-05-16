package flink.mystream.source

import org.apache.flink.streaming.api.scala.StreamExecutionEnvironment
import org.apache.flink.streaming.api.scala._
import org.apache.flink.types.LongValue
import org.apache.flink.util.LongValueSequenceIterator

object T1_ParallelSourceCollect {

  def main(args: Array[String]): Unit = {

    val environment: StreamExecutionEnvironment = StreamExecutionEnvironment.getExecutionEnvironment

    environment.fromElements(1,2,3)

    val value: DataStream[LongValue] = environment.fromParallelCollection(new LongValueSequenceIterator(1, 10)).setParallelism(4)

    val value1: DataStream[Long] = value.map(_.getValue.*(2)).setParallelism(3)

    val value2: DataStream[Long] = value1.filter(_ > 10)

    println("value" + "\t" + value.parallelism)
    println("value1" + "\t" + value1.parallelism)
    println("value2" + "\t" + value2.parallelism)

    value2.print()
    environment.execute()
  }
}
