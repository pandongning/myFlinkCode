package flink.mystream.operator.keyby

import org.apache.flink.api.common.functions.RichMapFunction
import org.apache.flink.api.java.tuple.Tuple
import org.apache.flink.api.scala._
import org.apache.flink.streaming.api.functions.KeyedProcessFunction
import org.apache.flink.streaming.api.scala.{DataStream, KeyedStream, StreamExecutionEnvironment}
import org.apache.flink.util.Collector

object T8_TupleKeyBy {

  def main(args: Array[String]): Unit = {

    val environment: StreamExecutionEnvironment = StreamExecutionEnvironment.getExecutionEnvironment

    val tupleDataStream: DataStream[(String, Int)] = environment.fromElements(("a", 1), ("a", 2), ("b", 1))

    //    指定tuple里面的第一个元素为key
    //    对于tuple，此处是使用位置参数，指定按照那个位置的元素进行聚合，按照那个位置的元素进行累加
    //    对DataStream进行keyBy，返回的类型为KeyedStream，其里面的都一个参数(String, Int)是流里面每个元素的参数类型
    //    第二个参数Tuple是key的类型。  对tuple进行keyBy操做的时候，如果key使用字段表达式或者位置参数，其返回的key类型固定为Tuple
    //    如果要返回其它类型的key，则需要使用keySelector
    val keyedStream: KeyedStream[(String, Int), Tuple] = tupleDataStream.keyBy(0)

    keyedStream.process(new MykeyedProcessFunction).print()

    /**
     * 输出的结果如下，可以看出a前面的线程id都为6，所以说明此时经过keyBy，其已经可以做到将key相同的发送到同一个线程里面
     * 处理
     * 6> a
     * 2> b
     * 6> a
     */

//    keyedStream.sum(1).print()

    environment.execute()
  }
}

class MykeyedProcessFunction extends KeyedProcessFunction[Tuple,(String, Int),String]{
  override def processElement(value: (String, Int), ctx: KeyedProcessFunction[Tuple, (String, Int), String]#Context, out: Collector[String]): Unit = {
    val key: Tuple = ctx.getCurrentKey
    val str: String = key.getField[String](0)
    out.collect(str)
  }
}


