package flink.mystream.operator

import org.apache.flink.streaming.api.scala.extensions._
import org.apache.flink.streaming.api.scala._

object T11_RichFilter {

  import org.apache.flink.api.common.functions.FilterFunction

  def main(args: Array[String]): Unit = {
    val environment: StreamExecutionEnvironment = StreamExecutionEnvironment.getExecutionEnvironment
    var value: DataStream[String] = environment.fromElements[String]("pdnpp", "pp", "dd")

    value.filter(new MyFliterFuncation("pp")).print()

    environment.execute()

  }

  class MyFliterFuncation(word:String) extends FilterFunction[String]{
    override def filter(value: String): Boolean = {
      value.contains(word)
    }
  }

}
