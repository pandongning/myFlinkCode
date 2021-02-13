package flink.mystream.operator

import org.apache.flink.api.common.functions.RichMapFunction
import org.apache.flink.api.scala._
import org.apache.flink.configuration.Configuration
import org.apache.flink.streaming.api.scala.{DataStream, StreamExecutionEnvironment}

object T10_RichMap {
  def main(args: Array[String]): Unit = {
    val environment: StreamExecutionEnvironment = StreamExecutionEnvironment.getExecutionEnvironment
    val value: DataStream[Int] = environment.fromElements(1, 2, 3)

    value.map(new UserDefineRichMapFunction).print()

    environment.execute()
  }
}

class UserDefineRichMapFunction extends RichMapFunction[Int, String] {

  var i: Long = _

  //获取数据库的连接等操作,其在实例化UserDefineRichMapFunction的时候只执行一次
  override def open(parameters: Configuration): Unit = {
    //下面的代码告诉我们可以在此处得到一些上下文信息
    //    val aa: util.List[Nothing] = getRuntimeContext.getBroadcastVariable("aa")

    i += System.currentTimeMillis()
  }

  override def map(value: Int): String = {
    //    getRuntimeContext.getIndexOfThisSubtask获取当前的事件位于那个子任务上面
    value + "\t" + i + "\t" + getRuntimeContext.getIndexOfThisSubtask

  }

  override def close(): Unit = super.close()
}
