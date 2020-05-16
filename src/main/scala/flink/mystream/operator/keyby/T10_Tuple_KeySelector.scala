package flink.mystream.operator.keyby

import org.apache.flink.streaming.api.scala.{DataStream, KeyedStream, StreamExecutionEnvironment}
import org.apache.flink.api.scala._

object T10_Tuple_KeySelector {
  def main(args: Array[String]): Unit = {

    val environment: StreamExecutionEnvironment = StreamExecutionEnvironment.getExecutionEnvironment

    val tupleDataStream: DataStream[(String, Int)] = environment.fromElements(("a", 1), ("a", 2), ("b", 1))

    //    使用键选择器函数(keySelector)定义键，此时的key为泛型里面的第二个参数
    val keyedStream: KeyedStream[(String, Int), String] = tupleDataStream.keyBy(tupleTwo => tupleTwo._1)

    keyedStream.map(tupleTwo=>print(tupleTwo._1))

    keyedStream.sum(1).print()

    environment.execute()
  }
}
