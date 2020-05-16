package flink.mystream.sink.jdbc

import java.util.Properties
import flink.mystream.beans.SensorReading
import org.apache.flink.api.common.serialization.SimpleStringSchema
import org.apache.flink.streaming.api.scala.{DataStream, StreamExecutionEnvironment}
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer
import org.apache.flink.api.scala._

object JdbcSink {

  def main(args: Array[String]): Unit = {
    val environment: StreamExecutionEnvironment = StreamExecutionEnvironment.getExecutionEnvironment

    val properties: Properties = new Properties()
    properties.setProperty("bootstrap.servers", "LocalOne:9092")
    properties.setProperty("group.id", "pdn1")
    properties.setProperty("key.deserializer", "org.apache.kafka.common.serialization.StringDeserializer")
    properties.setProperty("value.deserializer", "org.apache.kafka.common.serialization.StringDeserializer")

    val flinkKafkaConsumer011: FlinkKafkaConsumer[String] = new FlinkKafkaConsumer[String]("first", new SimpleStringSchema(), properties)

    val dataStream: DataStream[String] = environment.addSource(flinkKafkaConsumer011)

    val sensorReadingDataStream: DataStream[SensorReading] = dataStream.map(
      line => {
        val strings: Array[String] = line.split(",")
        SensorReading(strings(0), strings(1).trim.toLong, strings(2).trim.toDouble)
      }
    )

    sensorReadingDataStream.print()

    sensorReadingDataStream.addSink(new MyJdbcSink)

    environment.execute()

  }
}
