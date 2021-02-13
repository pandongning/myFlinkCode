package flink.mystream.windows.t2_countwindow

import org.apache.flink.api.scala._
import org.apache.flink.streaming.api.scala.{DataStream, StreamExecutionEnvironment}

object CountTumblingWindow {

  def main(args: Array[String]): Unit = {
    val environment: StreamExecutionEnvironment = StreamExecutionEnvironment.getExecutionEnvironment

    val line: DataStream[String] = environment.socketTextStream("LocalOne", 8888)

    /**
     *
     * [root@LocalOne ~]# nc -lk 8888
     * a,b,c
     * a,b
     * a,d  此处的输入的时候，对应的a的计算结果才会输出
     * 即同一个key的元素数量达到要求的时候才会被输出
     */
    line.flatMap((line: String) => line.split(",")).map(((_: String), 1)).keyBy(0)
      .countWindow(3)
      .sum(1).print()

    environment.execute()
  }

}
