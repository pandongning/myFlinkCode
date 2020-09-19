package flink.mystream.operator.keyby

import org.apache.flink.api.java.tuple.Tuple
import org.apache.flink.streaming.api.scala.{DataStream, KeyedStream, StreamExecutionEnvironment}
import org.apache.flink.api.scala._


object T4_CaseClass_FieldKeySelector {
  def main(args: Array[String]): Unit = {

    val environment: StreamExecutionEnvironment = StreamExecutionEnvironment.getExecutionEnvironment

    val WcDataStream: DataStream[WC] = environment.fromElements(WC("hello", 1), WC("word", 1), WC("hello", 2))

    //    使用键选择器函数定义键。此处直接指定case class里面参数的名字即可
    //    泛型参数Tuple表示key的类型
    val wordCounts: KeyedStream[WC, Tuple] = WcDataStream.keyBy("word")

    //    使用字段表达式指定聚合那个字段的值
    wordCounts.sum("count").print()

    environment.execute()
  }
}
