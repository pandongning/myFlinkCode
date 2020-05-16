package flink.mystream.utils

import java.util.Properties

import org.apache.flink.api.common.serialization.SimpleStringSchema
import org.apache.flink.streaming.api.scala.{DataStream, StreamExecutionEnvironment}
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer
import org.apache.flink.streaming.api.scala._

object FlinkKafkaUtil {

  private val environment: StreamExecutionEnvironment = StreamExecutionEnvironment.getExecutionEnvironment

  def createKafkaSoureStream(args: Array[String]): DataStream[String] = {
    val topic: String = args(0)
    val groupId: String = args(1)
    val brokerList: String = args(2)

    val properties: Properties = new Properties()
    properties.setProperty("bootstrap.servers", brokerList)
    properties.setProperty("group.id", groupId)
    properties.setProperty("key.deserializer", "org.apache.kafka.common.serialization.StringDeserializer")
    properties.setProperty("value.deserializer", "org.apache.kafka.common.serialization.StringDeserializer")
    val value: FlinkKafkaConsumer[String] = new FlinkKafkaConsumer[String](topic, new SimpleStringSchema(), properties)
//    一般应该在此处设置水印
    environment.addSource(value)
  }

  def getEnvironment: StreamExecutionEnvironment = {
    environment
  }
}
