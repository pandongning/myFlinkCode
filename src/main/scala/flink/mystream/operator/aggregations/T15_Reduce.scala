package flink.mystream.operator.aggregations

import flink.mystream.operator.keyby.SensorReading
import org.apache.flink.streaming.api.scala.{DataStream, KeyedStream, StreamExecutionEnvironment}
import org.apache.flink.api.scala._

object T15_Reduce {
  def main(args: Array[String]): Unit = {
    val environment: StreamExecutionEnvironment = StreamExecutionEnvironment.getExecutionEnvironment

    val inputStream: DataStream[String] = environment.readTextFile("src/main/resources/sensor.txt")

    val mapStream: DataStream[SensorReading] = inputStream.map(
      (item: String) => {
        val items: Array[String] = item.split(",")
        SensorReading(items(0).trim, items(1).trim.toLong, items(2).trim.toDouble)
      }
    )

    val keyStream: KeyedStream[SensorReading, String] = mapStream.keyBy((sensorReading: SensorReading) => sensorReading.id)

    val reduceDataStream: DataStream[SensorReading] = keyStream.reduce {
      (SensorReadingOne: SensorReading, SensorReadingTwo: SensorReading) => {
        SensorReading(SensorReadingOne.id, SensorReadingTwo.timestamp, SensorReadingTwo.temperature + SensorReadingOne.temperature)
      }
    }

    reduceDataStream.print()

    environment.execute("T2_Reduce")
  }
}
