package flink.mystream.operator

import flink.mystream.operator.keyby.SensorReading
import org.apache.flink.streaming.api.scala.{DataStream, SplitStream, StreamExecutionEnvironment}

object T5_Union {
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

    //    union要求两个流里面的参数类型必须一致
    val unionDataStream: DataStream[SensorReading] = highTempStream.union(lowTempStream)

    val mapUnionDataStream: DataStream[String] = unionDataStream.map((sensorReading: SensorReading) => (sensorReading.temperature + sensorReading.id))

    mapUnionDataStream.print()

    environment.execute()
  }
}
