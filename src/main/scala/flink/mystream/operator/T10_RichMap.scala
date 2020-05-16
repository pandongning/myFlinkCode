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

class UserDefineRichMapFunction extends RichMapFunction[Int, Int] {


  override def open(parameters: Configuration): Unit = {

  }

  override def map(value: Int): Int = {
    value + 100
  }

  override def close(): Unit = super.close()
}
