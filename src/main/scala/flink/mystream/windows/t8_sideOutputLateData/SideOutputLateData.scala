package flink.mystream.windows.t8_sideOutputLateData

import java.util.Properties

import flink.mystream.beans.SensorReading
import org.apache.flink.api.common.serialization.SimpleStringSchema
import org.apache.flink.streaming.api.TimeCharacteristic
import org.apache.flink.streaming.api.functions.timestamps.BoundedOutOfOrdernessTimestampExtractor
import org.apache.flink.streaming.api.scala.{StreamExecutionEnvironment, _}
import org.apache.flink.streaming.api.windowing.time.Time
import org.apache.flink.streaming.api.windowing.windows.TimeWindow
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer

object SideOutputLateData {

  def main(args: Array[String]): Unit = {

    val environment: StreamExecutionEnvironment = StreamExecutionEnvironment.getExecutionEnvironment
    environment.setStreamTimeCharacteristic(TimeCharacteristic.EventTime)

    val properties = new Properties()
    properties.setProperty("bootstrap.servers", "LocalOne:9092")
    properties.setProperty("group.id", "pdn2")
    properties.setProperty("key.deserializer", "org.apache.kafka.common.serialization.StringDeserializer")
    properties.setProperty("value.deserializer", "org.apache.kafka.common.serialization.StringDeserializer")

    val flinkKafkaConsumer: FlinkKafkaConsumer[String] = new FlinkKafkaConsumer[String]("first", new SimpleStringSchema(), properties)
    flinkKafkaConsumer.setStartFromLatest()

    val lineSource: DataStream[String] = environment.addSource(flinkKafkaConsumer)

    val sensorReading: DataStream[SensorReading] = lineSource.map(line => {
      val strings: Array[String] = line.split(",")
      SensorReading(strings(0), strings(1).trim.toLong, strings(2).trim.toDouble)
    }
    ).assignTimestampsAndWatermarks(new BoundedOutOfOrdernessTimestampExtractor[SensorReading](Time.milliseconds(1)) {
      override def extractTimestamp(element: SensorReading): Long = element.timestamp
    })

    val keyedStream: KeyedStream[SensorReading, String] = sensorReading.keyBy(_.id)

    val lateOutputTag: OutputTag[SensorReading] = new OutputTag[SensorReading]("lateData")

    val value: WindowedStream[SensorReading, String, TimeWindow] = keyedStream
      .timeWindow(Time.milliseconds(3))
      .allowedLateness(Time.milliseconds(2))
      .sideOutputLateData(lateOutputTag)


    val result: DataStream[SensorReading] = value
      .reduce((sensorReadingAgg, sensorReadingElement) => {
        SensorReading(sensorReadingAgg.id, sensorReadingAgg.timestamp, sensorReadingAgg.temperature + sensorReadingElement.temperature)
      })

    result.print()

//   收集迟到的数据，迟到的数据又会组成一个DataStream，此时则可以根据自己的业务逻辑进行处理
    val lateDate: DataStream[SensorReading] = result.getSideOutput(lateOutputTag)
    lateDate.print()

    environment.execute()

  }
}
