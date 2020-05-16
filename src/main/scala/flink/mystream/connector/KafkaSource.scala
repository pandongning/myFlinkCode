package flink.mystream.connector

import java.util.Properties

import org.apache.flink.api.common.JobExecutionResult
import org.apache.flink.api.common.serialization.SimpleStringSchema
import org.apache.flink.api.java.tuple.Tuple
import org.apache.flink.streaming.api.scala.{DataStream, KeyedStream, StreamExecutionEnvironment}
import org.apache.flink.api.scala._
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer

object KafkaSource {

  def main(args: Array[String]): Unit = {

    val env: StreamExecutionEnvironment = StreamExecutionEnvironment.getExecutionEnvironment
    //    给所有的算子设置相同的并行度。具体可以算子还可以覆盖这个默认的值
        env.setParallelism(4)

    val properties = new Properties()
    properties.setProperty("bootstrap.servers", "LocalOne:9092")
    properties.setProperty("group.id", "pdn1")
    properties.setProperty("key.deserializer", "org.apache.kafka.common.serialization.StringDeserializer")
    properties.setProperty("value.deserializer", "org.apache.kafka.common.serialization.StringDeserializer")


    val flinkKafkaConsumer: FlinkKafkaConsumer[String] = new FlinkKafkaConsumer[String]("first", new SimpleStringSchema(), properties)

    flinkKafkaConsumer.setStartFromLatest()

    val kafkaDataStream: DataStream[String] = env.addSource(flinkKafkaConsumer)

    val arrayDataStream: DataStream[Array[String]] = kafkaDataStream.map(
      line => {
        line.split(" ")
      }
    )

    val arrayKeyedStream: KeyedStream[Array[String], Tuple] = arrayDataStream.keyBy(0)
    val value: DataStream[String] = arrayKeyedStream.min(1).map(array => array.mkString(","))
    value.print()

    println("kafkaDataStream" + "\t" + kafkaDataStream.parallelism)
    println("value" + "\t" + value.parallelism)

    val jobExecutionResult: JobExecutionResult = env.execute("Kafka")
  }
}
