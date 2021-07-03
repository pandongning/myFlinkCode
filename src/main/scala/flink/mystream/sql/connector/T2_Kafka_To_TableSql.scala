package flink.mystream.sql.connector

import java.util.Properties

import org.apache.flink.table.api.scala._
import org.apache.flink.api.scala._
import flink.mystream.beans.{SensorReading, SensorReadingTwo}
import org.apache.flink.api.common.restartstrategy.RestartStrategies
import org.apache.flink.api.common.serialization.SimpleStringSchema
import org.apache.flink.streaming.api.{CheckpointingMode, TimeCharacteristic}
import org.apache.flink.streaming.api.environment.CheckpointConfig.ExternalizedCheckpointCleanup
import org.apache.flink.streaming.api.functions.timestamps.BoundedOutOfOrdernessTimestampExtractor
import org.apache.flink.streaming.api.scala.{DataStream, StreamExecutionEnvironment}
import org.apache.flink.streaming.api.windowing.time.Time
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer
import org.apache.flink.table.api.{EnvironmentSettings, Table}
import org.apache.flink.types.Row

object T2_Kafka_To_TableSql {

  def main(args: Array[String]): Unit = {

    val environment: StreamExecutionEnvironment = StreamExecutionEnvironment.getExecutionEnvironment
    val environmentSettings: EnvironmentSettings = EnvironmentSettings.newInstance().inStreamingMode().useBlinkPlanner().build()
    val tableEnvironment: StreamTableEnvironment = StreamTableEnvironment.create(environment, environmentSettings)

    environment.enableCheckpointing(40000L)
    environment.getCheckpointConfig.setCheckpointingMode(CheckpointingMode.EXACTLY_ONCE)
    environment.getCheckpointConfig.setCheckpointTimeout(10000L)
    environment.getCheckpointConfig.setFailOnCheckpointingErrors(false)
    environment.getCheckpointConfig.setMaxConcurrentCheckpoints(1)
    //    当任务最终失败或者被取消的时候，保留外部的ck。这样则可以自己检查任务，然后手动的恢复
    environment.getCheckpointConfig.enableExternalizedCheckpoints(ExternalizedCheckpointCleanup.RETAIN_ON_CANCELLATION)

    import org.apache.flink.runtime.state.filesystem.FsStateBackend
    environment.setStateBackend(new FsStateBackend("file:///D:/checkpoints"))

    //    失败之后自动重启3次，每次的间隔设置为120s。超过3次则直接判定任务失败。
    environment.setRestartStrategy(RestartStrategies.fixedDelayRestart(3, org.apache.flink.api.common.time.Time.seconds(120)))

    //    设置使用那种时间
    environment.setStreamTimeCharacteristic(TimeCharacteristic.EventTime)
    //  设置多长的间隔分配watermaker。只有使用周期性分配策略的时候，才需要此处的配置
    environment.getConfig.setAutoWatermarkInterval(500)


    val properties = new Properties()
    properties.setProperty("bootstrap.servers", "LocalOne:9092")
    properties.setProperty("group.id", "pdn1")
    properties.setProperty("key.deserializer", "org.apache.kafka.common.serialization.StringDeserializer")
    properties.setProperty("value.deserializer", "org.apache.kafka.common.serialization.StringDeserializer")


    //    下面的构造函数有许多的类型，具体可以自己看看，比如一次订阅kafka的多个topic
    //
    val flinkKafkaConsumerFirst: FlinkKafkaConsumer[String] = new FlinkKafkaConsumer[String]("first", new SimpleStringSchema(), properties)

    flinkKafkaConsumerFirst.setStartFromLatest()

    //    当ck成功之后，则提交便宜量给kafka
    flinkKafkaConsumerFirst.setCommitOffsetsOnCheckpoints(true)

    val kafkaDataStream: DataStream[String] = environment.addSource(flinkKafkaConsumerFirst)

    val value: DataStream[SensorReading] = kafkaDataStream.map(
      (line: String) => {
        val dataArray: Array[String] = line.split(",")
        // 转成String方便序列化输出.因为下面的FlinkKafkaProducer011里面定义的scheme为SimpleStringSchema，所以此处必须将流里面的元素变为String
        SensorReading(dataArray(0), dataArray(1).trim.toLong, dataArray(2).trim.toDouble)
      }
    ).assignTimestampsAndWatermarks(new BoundedOutOfOrdernessTimestampExtractor[SensorReading](Time.seconds(1)) {
      override def extractTimestamp(element: SensorReading): Long = {
        element.timestamp * 1000L
      }
    })

    val tableFirst: Table = tableEnvironment.fromDataStream(value, 'id, 'timestamp.rowtime as 'ts, 'temperature)

    tableEnvironment.createTemporaryView("fisrt", tableFirst)




    //    单条流的聚合.flinkSql的窗口对于延迟的数据，不会再次触发窗口。直接就丢弃呢
    //    val table: Table = tableEnvironment
    //      .sqlQuery(
    //        s"""
    //           |select id,sum(temperature),
    //           |TUMBLE_START(`ts`, INTERVAL '2' SECOND) as wStart,
    //           |TUMBLE_END(`ts`, INTERVAL '2' SECOND) as wEnd
    //           |from fisrt
    //           |group by TUMBLE(`ts`, INTERVAL '2' SECOND),id
    //           |""".stripMargin)

    //    table.toRetractStream[Row].print()


    //    双流join，然后再按照窗口聚合
    val propertiesTwo = new Properties()
    propertiesTwo.setProperty("bootstrap.servers", "LocalOne:9092")
    propertiesTwo.setProperty("group.id", "pdn2")
    propertiesTwo.setProperty("key.deserializer", "org.apache.kafka.common.serialization.StringDeserializer")
    propertiesTwo.setProperty("value.deserializer", "org.apache.kafka.common.serialization.StringDeserializer")


    //    下面的构造函数有许多的类型，具体可以自己看看，比如一次订阅kafka的多个topic
    val flinkKafkaConsumerSecond: FlinkKafkaConsumer[String] = new FlinkKafkaConsumer[String]("second", new SimpleStringSchema(), propertiesTwo)

    flinkKafkaConsumerSecond.setStartFromLatest()

    //    当ck成功之后，则提交便宜量给kafka
    flinkKafkaConsumerSecond.setCommitOffsetsOnCheckpoints(true)

    val kafkaDataStreamSecond: DataStream[String] = environment.addSource(flinkKafkaConsumerSecond)

    val valueSenond: DataStream[SensorReadingTwo] = kafkaDataStreamSecond.map(
      (line: String) => {
        val dataArray: Array[String] = line.split(",")
        // 转成String方便序列化输出.因为下面的FlinkKafkaProducer011里面定义的scheme为SimpleStringSchema，所以此处必须将流里面的元素变为String
        SensorReadingTwo(dataArray(0), dataArray(1).trim.toLong, dataArray(2).trim)
      }
    ).assignTimestampsAndWatermarks(new BoundedOutOfOrdernessTimestampExtractor[SensorReadingTwo](Time.seconds(1)) {
      override def extractTimestamp(element: SensorReadingTwo): Long = {
        element.timestamp * 1000L
      }
    })

    val tableSecond: Table = tableEnvironment.fromDataStream(valueSenond, 'id, 'timestamp.rowtime as 'ts, 'name)

    tableEnvironment.createTemporaryView("seocnd", tableSecond)


//    下面的语法是错误的，不能这样使用窗口的操作
    tableEnvironment.sqlQuery(
      s"""
         |select seocnd.name,sum(fisrt.temperature),
         |TUMBLE_START(fisrt.ts, INTERVAL '2' SECOND) as wStart,
         |TUMBLE_END(fisrt.ts, INTERVAL '2' SECOND) as wEnd
         |from fisrt left join seocnd on fisrt.id=seocnd.id
         |group by TUMBLE(fisrt.ts, INTERVAL '2' SECOND),seocnd.name
         |""".stripMargin).toRetractStream[Row].print()

    environment.execute()


    //    单条流的聚合
    //    val table: Table = tableEnvironment
    //      .sqlQuery(
    //        s"""
    //           |select id,sum(temperature),
    //           |TUMBLE_START(`ts`, INTERVAL '2' SECOND) as wStart,
    //           |TUMBLE_END(`ts`, INTERVAL '2' SECOND) as wEnd
    //           |from fisrt
    //           |group by TUMBLE(`ts`, INTERVAL '2' SECOND),id
    //           |""".stripMargin)

    //    table.toRetractStream[Row].print()


  }

}
