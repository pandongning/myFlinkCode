package flink.mystream.operator

import org.apache.flink.streaming.api.scala.{ConnectedStreams, DataStream, StreamExecutionEnvironment}
import org.apache.flink.api.scala._

object T4_Connect_DiffType {

  def main(args: Array[String]): Unit = {

    val environment: StreamExecutionEnvironment = StreamExecutionEnvironment.getExecutionEnvironment

    val intDataStream: DataStream[Int] = environment.fromElements(1, 2, 3)

    val stringDataStream: DataStream[String] = environment.fromElements("a", "b")

    val connectedStreams: ConnectedStreams[Int, String] = intDataStream.connect(stringDataStream)

    var count: Int = 0

    val value: DataStream[Any] = connectedStreams.map(
      i => {
        count += 1
        i + 100
      },
      string => {
        count += 1
        string + "_" + "pdn"
      }
    )

    value.print()

    environment.execute()
  }
}
