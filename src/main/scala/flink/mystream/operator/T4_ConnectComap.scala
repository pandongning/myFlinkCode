package flink.mystream.operator

import flink.mystream.operator.keyby.SensorReading
import org.apache.flink.streaming.api.scala.{ConnectedStreams, DataStream, SplitStream, StreamExecutionEnvironment}

object T4_ConnectComap {

  def main(args: Array[String]): Unit = {

    val environment: StreamExecutionEnvironment = StreamExecutionEnvironment.getExecutionEnvironment

    val inputStream: DataStream[String] = environment.readTextFile("src/main/resources/sensor.txt")

    import org.apache.flink.api.scala._
    val mapStream: DataStream[SensorReading] = inputStream.map(
      (item: String) => {
        val items: Array[String] = item.split(",")
        SensorReading(items(0).trim, items(1).trim.toLong, items(2).trim.toDouble)
      }
    )

    val splitStream: SplitStream[SensorReading] = mapStream.split(
      (sensorReading: SensorReading) => {
        if (sensorReading.temperature > 2) Seq("high") else Seq("low")
      }
    )

    val highTempStream: DataStream[SensorReading] = splitStream.select("high")
    val lowTempStream: DataStream[SensorReading] = splitStream.select("low")

    //两个泛型分别表示每个流里面的元素类型
    val connectedStreams: ConnectedStreams[SensorReading, SensorReading] = highTempStream.connect(lowTempStream)

//    此处的map就是comap
    val connectedStream: DataStream[String] = connectedStreams.map(
      ((sensorReading: SensorReading) => ("high_" + sensorReading.temperature)),
      ((sensorReading: SensorReading) => ("low_" + sensorReading.temperature))
    )

    connectedStream.print()

    environment.execute("T4_Connect")

  }
}
