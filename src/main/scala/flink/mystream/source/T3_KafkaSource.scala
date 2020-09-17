package flink.mystream.source

import java.util.Properties

import org.apache.flink.api.common.JobExecutionResult
import org.apache.flink.api.common.serialization.SimpleStringSchema
import org.apache.flink.streaming.api.scala.{DataStream, KeyedStream, StreamExecutionEnvironment, _}
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer

object T3_KafkaSource {


  def main(args: Array[String]): Unit = {
    import org.slf4j.{Logger, LoggerFactory}

    val logger: Logger = LoggerFactory.getLogger(T3_KafkaSource.getClass)


    val env: StreamExecutionEnvironment = StreamExecutionEnvironment.getExecutionEnvironment
    //    给所有的算子设置相同的并行度。具体可以算子还可以覆盖这个默认的值。
    //    因为设置了此值，则下面的kafkaDataStream的并行度也为4.
    //    但是如果在此处不设置，而是在下面设置，则kafkaDataStream并行度不会受到其影响。
    //    如果不设置值，则如果在idea本地运行则其并行度为本机线程的核数
    //    如果是在集群上运行
    //    env.setParallelism(4)

    val properties = new Properties()
    properties.setProperty("bootstrap.servers", "LocalOne:9092")
    properties.setProperty("group.id", "pdn1")
    properties.setProperty("key.deserializer", "org.apache.kafka.common.serialization.StringDeserializer")
    properties.setProperty("value.deserializer", "org.apache.kafka.common.serialization.StringDeserializer")


    val flinkKafkaConsumer: FlinkKafkaConsumer[String] = new FlinkKafkaConsumer[String]("first", new SimpleStringSchema(), properties)

    flinkKafkaConsumer.setStartFromLatest()
    //    当ck成功之后，则提交便宜量给kafka
    flinkKafkaConsumer.setCommitOffsetsOnCheckpoints(true)

    val kafkaDataStream: DataStream[String] = env.addSource(flinkKafkaConsumer)
    logger.error("kafkaDataStream" + "\t" + kafkaDataStream.parallelism)


    //    在此处设置的值，不会对其以前的算子产生影响
//    env.setParallelism(4)

    val arrayDataStream: DataStream[(String, Int)] = kafkaDataStream.flatMap(
      (line: String) => {
        line.split(" ")
      }

    ).map((word: String) => (word, 1))
    logger.error("arrayDataStream" + "\t" + arrayDataStream.parallelism)


    val arrayKeyedStream: KeyedStream[(String, Int), String] = arrayDataStream.keyBy((_: (String, Int))._1)
//    由于上面在代码里面指定了全局的并行度，则此处的hash之后的并行度也是4
    logger.error("arrayKeyedStream" + "\t" + arrayKeyedStream.parallelism)

    val value: DataStream[(String, Int)] = arrayKeyedStream.sum(1)
    logger.error("value" + "\t" + value.parallelism)

    value.print()

    println("kafkaDataStream" + "\t" + kafkaDataStream.parallelism)
    println("arrayDataStream"+"\t"+arrayDataStream.parallelism)
    println("arrayKeyedStream"+"\t"+arrayDataStream.parallelism)
    println("value" + "\t" + value.parallelism)

    val jobExecutionResult: JobExecutionResult = env.execute("MyKafkaSoure")
  }
}
