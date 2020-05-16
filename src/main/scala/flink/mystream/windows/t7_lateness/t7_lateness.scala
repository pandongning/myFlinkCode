package flink.mystream.windows.t7_lateness

import java.util.Properties

import flink.mystream.beans.SensorReading
import org.apache.flink.api.common.serialization.SimpleStringSchema
import org.apache.flink.streaming.api.TimeCharacteristic
import org.apache.flink.streaming.api.functions.timestamps.BoundedOutOfOrdernessTimestampExtractor
import org.apache.flink.streaming.api.scala.function.ProcessWindowFunction
import org.apache.flink.streaming.api.scala.{StreamExecutionEnvironment, _}
import org.apache.flink.streaming.api.windowing.time.Time
import org.apache.flink.streaming.api.windowing.windows.TimeWindow
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer
import org.apache.flink.util.Collector

object t7_lateness {

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

    keyedStream.timeWindow(Time.milliseconds(3))
      .allowedLateness(Time.milliseconds(2))
      .process(new T7_ProcessWindowFunction)
      .print()

    environment.execute()
  }
}

class T7_ProcessWindowFunction extends ProcessWindowFunction[SensorReading, String, String, TimeWindow] {

  override def process(key: String, context: Context, elements: Iterable[SensorReading], out: Collector[String]): Unit = {
    val elementsIterator: Iterator[SensorReading] = elements.iterator

    var sum: Double = 0

    while (elementsIterator.hasNext) {
      val sensorReading: SensorReading = elementsIterator.next()
      sum += sensorReading.temperature
    }

    out.collect(key + "\t" + context.window.getEnd + "\t" + sum)
  }
}
