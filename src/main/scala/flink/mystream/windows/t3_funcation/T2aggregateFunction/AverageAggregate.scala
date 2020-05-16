package flink.mystream.windows.t3_funcation.T2aggregateFunction

import org.apache.flink.api.common.functions.AggregateFunction

/**
 * 从此处可以看出reduce就是特殊的AggregateFunction
 * 对于reduce，其累加器的类型和输入的元素类型必须一致，
 * 但是对于AggregateFunction累加器的类型和输入的元素类型可以不一致
 *
 * < IN>  The type of the values that are aggregated (input values)
 * < ACC> The type of the accumulator (intermediate aggregate state).
 * < OUT> The type of the aggregated result
 **/


class AverageAggregate extends AggregateFunction[(String, Long), (Long, Long), Double] {

  //  创建累加器的初始值
  override def createAccumulator(): (Long, Long) = (0L, 0L)

  //  使用当前处理的event，更新累加器的值
  override def add(value: (String, Long), accumulator: (Long, Long)): (Long, Long) =
    (accumulator._1 + value._2, accumulator._2 + 1L)

  //  从累加器里面获得结果
  override def getResult(accumulator: (Long, Long)): Double = accumulator._1 / accumulator._2

  //  merge类似于对不同的taskManager里面的累加器进行合并
  override def merge(a: (Long, Long), b: (Long, Long)): (Long, Long) =
    (a._1 + b._1, a._2 + b._2)
}
