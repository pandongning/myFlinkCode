package flink.mystream.windows.t2_countwindow

import org.apache.flink.api.scala._
import org.apache.flink.streaming.api.scala.{DataStream, StreamExecutionEnvironment}

object CountSlideWindow {

  def main(args: Array[String]): Unit = {

    val environment: StreamExecutionEnvironment = StreamExecutionEnvironment.getExecutionEnvironment

    val line: DataStream[String] = environment.socketTextStream("LocalOne", 8888)

    /**
     * 输入
     * a,c
     * c,d
     * 则输出。* 所以此时可以看到，其是每隔2个元素输出前面5个元素，即使前面没有5个元素，其也会有输出的数据
     * 4> (c,2)
     * 接着输入
     * a,b,c
     * 则输出
     * 6> (a,2)
     * 2> (b,2)
     * 接着输入
     * c,c,b
     * 输出
     * 4> (c,4)
     */

    line.flatMap((line: String) => line.split(",")).map(((_: String), 1)).keyBy(0)
      .countWindow(5, 2).sum(1).print()

    environment.execute()
  }

}
