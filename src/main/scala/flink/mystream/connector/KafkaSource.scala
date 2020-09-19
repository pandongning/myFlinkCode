package flink.mystream.connector

import java.util.Properties

import org.apache.flink.api.common.JobExecutionResult
import org.apache.flink.api.common.serialization.SimpleStringSchema
import org.apache.flink.api.scala._
import org.apache.flink.streaming.api.scala.{DataStream, StreamExecutionEnvironment}
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer

object KafkaSource {

  def main(args: Array[String]): Unit = {
    import org.apache.flink.streaming.api.scala

    val env: StreamExecutionEnvironment = StreamExecutionEnvironment.getExecutionEnvironment
    //    给所有的算子设置相同的并行度。具体可以算子还可以覆盖这个默认的值
    //    env.setParallelism(4)

    val properties = new Properties()
    properties.setProperty("bootstrap.servers", "LocalOne:9092")
    properties.setProperty("group.id", "pdn1")
    properties.setProperty("key.deserializer", "org.apache.kafka.common.serialization.StringDeserializer")
    properties.setProperty("value.deserializer", "org.apache.kafka.common.serialization.StringDeserializer")


//    下面的构造函数有许多的类型，具体可以自己看看，比如一次订阅kafka的多个topic
//
    val flinkKafkaConsumer: FlinkKafkaConsumer[String] = new FlinkKafkaConsumer[String]("first", new SimpleStringSchema(), properties)

    flinkKafkaConsumer.setStartFromLatest()
    //    当ck成功之后，则提交便宜量给kafka
    flinkKafkaConsumer.setCommitOffsetsOnCheckpoints(true)

    val kafkaDataStream: DataStream[String] = env.addSource(flinkKafkaConsumer)

    val arrayDataStream: DataStream[Array[String]] = kafkaDataStream.map(
      (line: String) => {
        line.split(" ")
      }
    )

    //    此处的字段1是将来要进行聚合的字段
    val value1: DataStream[(Array[String], Int)] = arrayDataStream.map((arrry: Array[String]) => (arrry, arrry(1).toInt))

    //    按照多个维度进行聚合
    val value2: scala.KeyedStream[(Array[String], Int), (String, String)] = value1.keyBy((tuple: (Array[String], Int)) => (tuple._1(0), tuple._1(2)))

    //将已经要聚合的字段值进行累加
    val value: DataStream[(Array[String], Int)] = value2.sum(1)

    val res: DataStream[String] = value.map((ele: (Array[String], Int)) => ele._1.mkString("Array(", ", ", ")") + "\t" + ele._2)
    res.print()


    println("kafkaDataStream" + "\t" + kafkaDataStream.parallelism)
    println("value" + "\t" + value.parallelism)

    val jobExecutionResult: JobExecutionResult = env.execute("Kafka")
  }
}
