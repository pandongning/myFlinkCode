package flink.mystream.windows.t1_timewindow

import java.util.Properties

import flink.mystream.beans.SensorReading
import flink.mystream.sink.jdbc.MyJdbcSink
import org.apache.flink.api.common.serialization.SimpleStringSchema
import org.apache.flink.streaming.api.TimeCharacteristic
import org.apache.flink.streaming.api.functions.timestamps.BoundedOutOfOrdernessTimestampExtractor
import org.apache.flink.streaming.api.scala.StreamExecutionEnvironment
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer
import org.apache.flink.streaming.api.scala._
import org.apache.flink.streaming.api.windowing.time.Time


object TumblingWindowsKafkaEventTime {

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


    val sensorReading: DataStream[SensorReading] = lineSource.map((line: String) => {
      val strings: Array[String] = line.split(",")
      SensorReading(strings(0), strings(1).trim.toLong, strings(2).trim.toDouble)
    }
    ).assignTimestampsAndWatermarks(new BoundedOutOfOrdernessTimestampExtractor[SensorReading](Time.milliseconds(1)) {
      override def extractTimestamp(element: SensorReading): Long = element.timestamp
    })

    val keyedStream: KeyedStream[SensorReading, String] = sensorReading.keyBy((_: SensorReading).id)

    //测试窗口的开始时间，和kafka做为source的时候的窗口开始执行的时间
    val value: DataStream[SensorReading] = keyedStream.timeWindow(Time.milliseconds(2)).reduce(
      (sensorReadingAgg: SensorReading, sensorReadingElement: SensorReading) => {
        SensorReading(sensorReadingAgg.id, sensorReadingAgg.timestamp, sensorReadingAgg.temperature + sensorReadingElement.temperature)
      }
    )

    value.addSink(new MyJdbcSink())

    value.print()


    environment.execute()
  }
}
