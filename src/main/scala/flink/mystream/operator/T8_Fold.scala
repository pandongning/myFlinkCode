package flink.mystream.operator

import org.apache.flink.streaming.api.scala.{DataStream, KeyedStream, StreamExecutionEnvironment}
import org.apache.flink.api.scala._

object T8_Fold {

  def main(args: Array[String]): Unit = {

    val environment: StreamExecutionEnvironment = StreamExecutionEnvironment.getExecutionEnvironment

    val lineDataStream: DataStream[String] = environment.fromElements("aa bb", "aa dd")

    val wordDataStream: DataStream[String] = lineDataStream.flatMap((line: String) => line.split(" "))

    val wordKeyedStream: KeyedStream[String, String] = wordDataStream.keyBy((word: String) => word)

    //pdn是累加得初始值，stringOne和stringTwo表示流里面将要被累加得两个元素
    val value: DataStream[String] = wordKeyedStream.fold("pdn")((stringOne: String, stringTwo: String) => {
      stringOne + "_" + stringTwo
    })

    value.print()

    /**
     * 5> pdn_aa
     * 8> pdn_dd
     * 5> pdn_aa_aa  此处是因为按照key进行了聚合，key相同得元素，被发送到了一起
     * 5> pdn_bb
     */

    environment.execute()
  }
}
