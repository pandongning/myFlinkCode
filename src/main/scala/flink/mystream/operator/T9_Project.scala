package flink.mystream.operator

import flink.mystream.join.Transcript
import org.apache.flink.api.java.tuple.Tuple4
import org.apache.flink.streaming.api.datastream
import org.apache.flink.streaming.api.scala.StreamExecutionEnvironment
import org.apache.flink.streaming.api.scala._

object T9_Project {
  def main(args: Array[String]): Unit = {
    val environment: StreamExecutionEnvironment = StreamExecutionEnvironment.getExecutionEnvironment

    val value: DataStream[Tuple4[String, String, String, Integer]] = environment.fromCollection(TRANSCRIPT)

    //    project只有javaStream支持，用于从event里面提取个别的字段
    value.javaStream.project(0, 1).print()


    val stream: datastream.DataStream[Tuple4[String, String, String, Integer]] = value.javaStream


    environment.execute()


  }


  val TRANSCRIPT: Array[Tuple4[String, String, String, Integer]] = Array[Tuple4[String, String, String, Integer]](Tuple4.of("class1", "张三", "语文", 100), Tuple4.of("class1", "李四", "语文", 78), Tuple4.of("class1", "王五", "语文", 99), Tuple4.of("class2", "赵六", "语文", 81), Tuple4.of("class2", "钱七", "语文", 59), Tuple4.of("class2", "马二", "语文", 97))
}
