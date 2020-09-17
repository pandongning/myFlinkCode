package flink.mystream.operator.aggregations

import org.apache.flink.api.scala._
import org.apache.flink.streaming.api.scala.{DataStream, KeyedStream, StreamExecutionEnvironment}

/**
 * max和min是对应的
 */
object T13_min {

  def main(args: Array[String]): Unit = {

    val environment: StreamExecutionEnvironment = StreamExecutionEnvironment.getExecutionEnvironment

    val tupleDataStream: DataStream[(String, Int, String)] = environment.fromElements(("a", 1, "pdn1"), ("b", 3, "pdn2"), ("a", 3, "pdn3"), ("b", 3, "pdn1"), ("b", 4, "pdn4"))

    val tupleKeyedStream: KeyedStream[(String, Int, String), String] = tupleDataStream.keyBy((_: (String, Int, String))._1)

    val value: DataStream[(String, Int, String)] = tupleKeyedStream.max(1)

    value.print()

    /**
     * 最后的输出结果如下。所以对于KeyedStream的min是输出每个key对应的组里面最小的
     * 比如第一次输入是(a,1,pdn1)由于此时没有比较的对象，所以直接向外输出的是(a,1,pdn1)
     * 当处理到("a", 3, "pdn3")的是时候，则直接对比第二个元素的大小，此时3大于1，
     * 则本来应该输出的是("a", 3, "pdn3")
     * 但实际的输出是(a,3,pdn1)  所以其只是使用最大的元素3替换了1，其余的字段都没有变化
     *
     *
     * 6> (a,1,pdn1)
     * 2> (b,3,pdn2)
     * 2> (b,3,pdn2)
     * 6> (a,3,pdn1)
     * 2> (b,4,pdn2)
     */

    environment.execute()
  }
}
