package flink.mystream.sink

import java.util.Properties

import flink.mystream.beans.SensorReading
import org.apache.flink.api.common.serialization.SimpleStringSchema
import org.apache.flink.api.scala._
import org.apache.flink.streaming.api.scala.{DataStream, StreamExecutionEnvironment}
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaProducer

object KafkaSink {

  def main(args: Array[String]): Unit = {


    val environment: StreamExecutionEnvironment = StreamExecutionEnvironment.getExecutionEnvironment


    val properties: Properties = new Properties()
    properties.setProperty("bootstrap.servers", "LocalTwo:9092")
    properties.setProperty("group.id", "pdn2")
    properties.setProperty("key.deserializer", "org.apache.kafka.common.serialization.StringDeserializer")
    properties.setProperty("value.deserializer", "org.apache.kafka.common.serialization.StringDeserializer")
    properties.setProperty("transaction.timeout.ms", 1000 * 60 * 5 + "")

    val flinkKafkaConsumer: FlinkKafkaConsumer[String] = new FlinkKafkaConsumer[String]("first", new SimpleStringSchema(), properties)
    flinkKafkaConsumer.setCommitOffsetsOnCheckpoints(false)
    flinkKafkaConsumer.setStartFromLatest()

    val sourceDataStream: DataStream[String] = environment.addSource(flinkKafkaConsumer)

    val sensorReadingDataStream: DataStream[String] = sourceDataStream.map(
      (line: String) => {
        val dataArray: Array[String] = line.split(",")
        // 转成String方便序列化输出.因为下面的FlinkKafkaProducer011里面定义的scheme为SimpleStringSchema，所以此处必须将流里面的元素变为String
        SensorReading(dataArray(0), dataArray(1).trim.toLong, dataArray(2).trim.toDouble).toString
      }
    )

    //    泛型参数是流里面元素的类型
    //    如果不提供自定义的分区函数，则所有的数据被发送到kafka的一个分区里面
    //    如果提供了分区器，则所有的按照分区的规则，将数据发送到不同的分区里面
    //    如果没有提供分区器，则按照event里面的key字段。具体这部分看文档
    //    如果key值为null，则使用round-robin
    //    如果自定定义分区器，但是分区器在任务失败的时候不会保存状态，所以建议最好使用key为null，实现让每个分区里面都有数据
    //    个人觉得此处应该设置事务的超时时间大于1h，而不是小于15min
    properties.setProperty("transaction.timeout.ms", 1000 * 60 * 5 + "")
    val flinkKafkaProducer011 = new FlinkKafkaProducer("flinkSink", new SimpleStringSchema(), properties)


    //  因为kafkasink是两阶段提交，所以预提交的时间过长，则会导致事物的超时，此时则是设置
    //    因为事物超时导致的失败，不会进行重启
    flinkKafkaProducer011.ignoreFailuresAfterTransactionTimeout()


    sensorReadingDataStream.addSink(flinkKafkaProducer011)

    environment.execute()
  }
}
