package flink.mystream.windows.t9_consecutiveWindowedOperations

import java.util.Properties

import flink.mystream.beans.SensorReading
import org.apache.flink.api.common.serialization.SimpleStringSchema
import org.apache.flink.streaming.api.TimeCharacteristic
import org.apache.flink.streaming.api.functions.timestamps.BoundedOutOfOrdernessTimestampExtractor
import org.apache.flink.streaming.api.scala.function.ProcessAllWindowFunction
import org.apache.flink.streaming.api.scala.{StreamExecutionEnvironment, _}
import org.apache.flink.streaming.api.windowing.time.Time
import org.apache.flink.streaming.api.windowing.windows.TimeWindow
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer
import org.apache.flink.util.Collector

object T9_ConsecutiveWindowedOperations {

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

//    key相同的温度值相加
    val firstWindowResult: DataStream[SensorReading] =
      keyedStream.timeWindow(Time.milliseconds(5))
        .sum("temperature")

//    对于firstWindowResult的输出,按照温度值的高低进行排序输出
    val secondWindowResult: DataStream[List[SensorReading]] =
      firstWindowResult.timeWindowAll(Time.milliseconds(5))
        .process(new T9_ProcessWindowFunction)

    secondWindowResult.print()

    environment.execute()
  }
}

class T9_ProcessWindowFunction extends ProcessAllWindowFunction[SensorReading, List[SensorReading], TimeWindow] {

  override def process(context: Context, elements: Iterable[SensorReading], out: Collector[List[SensorReading]]): Unit = {
    val sensorReadings: List[SensorReading] = elements.toList.sortWith((sensorReadingOne, sensorReadingTwo) => {
      sensorReadingOne.temperature > sensorReadingTwo.temperature
    })

    out.collect(sensorReadings)
  }
}
