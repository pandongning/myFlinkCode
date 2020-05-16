package flink.mystream.operator

import org.apache.flink.streaming.api.scala.{DataStream, KeyedStream, StreamExecutionEnvironment}
import org.apache.flink.api.scala._

object T8_Fold {

  def main(args: Array[String]): Unit = {

    val environment: StreamExecutionEnvironment = StreamExecutionEnvironment.getExecutionEnvironment

        val lineDataStream: DataStream[String] = environment.fromElements("aa bb", "cc dd ee")

        val wordDataStream: DataStream[String] = lineDataStream.flatMap(line => line.split(" "))

        val wordKeyedStream: KeyedStream[String, String] = wordDataStream.keyBy(word => word)

        val value: DataStream[String] = wordKeyedStream.fold("pdn")((name, word) => {
          name + "_" + word
        })

        value.print()

    /**
     * 1> pdn_ee
     * 5> pdn_aa
     * 5> pdn_bb
     * 8> pdn_cc
     * 8> pdn_dd
     */

    environment.execute()
  }
}
