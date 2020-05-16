package flink.mystream.windows.t3_funcation

import org.apache.flink.streaming.api.scala.{DataStream, StreamExecutionEnvironment}
import org.apache.flink.streaming.api.windowing.time.Time

object T1_ReduceFunction {

  def main(args: Array[String]): Unit = {
    val env = StreamExecutionEnvironment.getExecutionEnvironment

    import org.apache.flink.api.scala._

    val text = env.socketTextStream("LocalOne", 8888)

    // 原来传递进来的数据是字符串，此处我们就使用数值类型，通过数值类型来演示增量的效果
    //    此处实现了窗口里面的单词计数
    text.flatMap(_.split(","))
      .map(x => (1, x.toInt)) // 1,2,3,4,5 ==> (1,1) (1,2) (1,3) (1,4) (1,5)
      .keyBy(0) // 因为key都是1，所以所有的元素都到一个task去执行
      .timeWindow(Time.seconds(5))
      .reduce((v1, v2) => { // 不是等待窗口所有的数据进行一次性处理，而是数据两两处理.
        // v1是上一次聚合的结果即累加器，v2是本次要处理的event。
        // reduec里面不一定要写聚合的逻辑。只要是本次处理的event和上次结果之间的逻辑计算即可
        // 比如其里面可以写
        println(v1 + " ... " + v2)
        (v1._1, v1._2 + v2._2)
      })
      .print()
      .setParallelism(1)

    env.execute("WindowsReduceApp")
  }
}
