package flink.mystream.operator.keyby

import org.apache.flink.streaming.api.scala.{DataStream, KeyedStream, StreamExecutionEnvironment}
import org.apache.flink.api.scala._

case class WC(word: String, count: Int)

object T3_CaseClass_KeySelector {
  def main(args: Array[String]): Unit = {

    val environment: StreamExecutionEnvironment = StreamExecutionEnvironment.getExecutionEnvironment

    val WcDataStream: DataStream[WC] = environment.fromElements(WC("hello", 1), WC("word", 1), WC("hello", 2))

//    使用键选择器函数定义键
    //    泛型参数String表示key的类型
    val keyedStream: KeyedStream[WC, String] = WcDataStream.keyBy(wc => wc.word)

//    使用字段表达式指定聚合那个字段的值
    keyedStream.sum("count").print()

    environment.execute()
  }
}
