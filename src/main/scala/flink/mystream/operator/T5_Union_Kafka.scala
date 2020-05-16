package flink.mystream.operator

import java.util.Properties

import org.apache.flink.api.common.functions.RichMapFunction
import org.apache.flink.api.common.serialization.SimpleStringSchema
import org.apache.flink.streaming.api.scala.{DataStream, StreamExecutionEnvironment}
import org.apache.flink.api.scala._
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer

object T5_Union_Kafka {

  def main(args: Array[String]): Unit = {

    val environment: StreamExecutionEnvironment = StreamExecutionEnvironment.getExecutionEnvironment

    environment.setParallelism(3)

    val dsOneProperties: Properties = new Properties()
    dsOneProperties.setProperty("bootstrap.servers", "LocalOne:9092")
    dsOneProperties.setProperty("group.id", "dsOne")
    dsOneProperties.setProperty("key.deserializer", "org.apache.kafka.common.serialization.StringDeserializer")
    dsOneProperties.setProperty("value.deserializer", "org.apache.kafka.common.serialization.StringDeserializer")


    val dsOne: FlinkKafkaConsumer[String] = new FlinkKafkaConsumer[String]("first", new SimpleStringSchema(), dsOneProperties)

    val dsOneDataStream: DataStream[String] = environment.addSource(dsOne)

    dsOneDataStream.map(mapper = new RichMapFunction[String, String] {

      override def map(value: String): String = "aa"
      override def close(): Unit = super.close()
    })

  }
}
