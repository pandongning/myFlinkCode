package flink.mystream.utils

import flink.mystream.beans.SensorReading
import org.apache.flink.streaming.api.TimeCharacteristic
import org.apache.flink.streaming.api.functions.timestamps.BoundedOutOfOrdernessTimestampExtractor
import org.apache.flink.streaming.api.scala.{DataStream, StreamExecutionEnvironment}
import org.apache.flink.table.api.{EnvironmentSettings, Over, Table, Tumble}
import org.apache.flink.table.api.scala.StreamTableEnvironment
import org.apache.flink.api.scala._
import org.apache.flink.table.api.scala._
import org.apache.flink.streaming.api.windowing.time.Time
import org.apache.flink.types.Row

object SensorReadingDataSource {

  var environment: StreamExecutionEnvironment = getStreamExecutionEnvironment

  def getDataSource(hostName:String,port:Int): DataStream[SensorReading] = {

    val sourceData: DataStream[String] = environment.socketTextStream(hostName, port)

    val sensorReadingDataStream: DataStream[SensorReading] = sourceData.map(line => {
      val strings: Array[String] = line.split(",")
      SensorReading(strings(0).trim, strings(1).trim.toLong, strings(2).trim.toDouble)
    }).assignTimestampsAndWatermarks(new BoundedOutOfOrdernessTimestampExtractor[SensorReading](Time.milliseconds(1)) {
      override def extractTimestamp(element: SensorReading): Long = element.timestamp
    })

    sensorReadingDataStream
  }


  def getStreamExecutionEnvironment: StreamExecutionEnvironment = {
    val environment: StreamExecutionEnvironment = StreamExecutionEnvironment.getExecutionEnvironment
    environment.setStreamTimeCharacteristic(TimeCharacteristic.EventTime)
    environment.setParallelism(3)
    environment
  }

  def getTableEnvironment: StreamTableEnvironment = {

    val environmentSettings: EnvironmentSettings = EnvironmentSettings.newInstance().inStreamingMode().useBlinkPlanner().build()
    val tableEnvironment: StreamTableEnvironment = StreamTableEnvironment.create(environment, environmentSettings)
    tableEnvironment
  }

  def main(args: Array[String]): Unit = {
    getTableEnvironment

  }
}
