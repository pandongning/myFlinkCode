package flink.mystream.utils
//
//import java.util
//import java.util.Properties
//
//import org.apache.flink.api.common.restartstrategy.RestartStrategies
//import org.apache.flink.api.common.serialization.{DeserializationSchema, SimpleStringSchema}
//import org.apache.flink.api.common.typeinfo.TypeInformation
//import org.apache.flink.api.java.utils.ParameterTool
//import org.apache.flink.contrib.streaming.state.RocksDBStateBackend
//import org.apache.flink.streaming.api.environment.CheckpointConfig.ExternalizedCheckpointCleanup
//import org.apache.flink.streaming.api.{CheckpointingMode, TimeCharacteristic}
//import org.apache.flink.streaming.api.scala.{DataStream, StreamExecutionEnvironment}
//import org.apache.flink.api.scala._
//import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer
//import org.apache.flink.streaming.api.scala.extensions._
//
////这个程序是错误的，其泛型无法被解析
object FlinkUtilsScalaVersion {
//
//  private val env: StreamExecutionEnvironment = StreamExecutionEnvironment.getExecutionEnvironment
//
//  //    设置使用eventTime
//  env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime)
//
//  //    配置ck的信息
//  env.enableCheckpointing(50000)
//  env.getCheckpointConfig.setCheckpointingMode(CheckpointingMode.AT_LEAST_ONCE)
//
//  //    因为Checkpoint的时候需要将状态信息保存到文件系统，或者是其他的存储文件里面。
//  //    所以其牵扯到IO的操做，所以此时可能出现超时的问题，如果ck的时候超过下面指定的时间，则抛弃
//  //    当前的ck。到下次时间间隔的时候再进行ck操做
//  env.getCheckpointConfig.setCheckpointTimeout(100000)
//
//  //    false表示，ck失败，不会停止整个job。
//  env.getCheckpointConfig.setFailOnCheckpointingErrors(false)
//
//  //    最大同时进行的ck数量，如果ck的间隔小于，超时的时间，则会导致，一个ck还未结束 另一个ck已经开启执行
//  env.getCheckpointConfig.setMaxConcurrentCheckpoints(1)
//
//  //  job失败时候，是否自动的删除外部持久化的ck信息。下面的配置表示删除
//  env.getCheckpointConfig.enableExternalizedCheckpoints(ExternalizedCheckpointCleanup.DELETE_ON_CANCELLATION)
//
//  //    设置失败重启的策略
//  //    失败之后自动重启3次，每次的间隔设置为120s
//  env.setRestartStrategy(RestartStrategies.fixedDelayRestart(3, org.apache.flink.api.common.time.Time.seconds(120)))
//
//  //    设置ck保存的目录。此处是设置的状态后端为RocksDB.
//  //    方法过时的原因是目前建议在全局配置里面配置，而不是在代码里面单独配置
//  env.setStateBackend(new RocksDBStateBackend("hdfs://LocalOne:9000/flink/checkpoints"))
//
//  def createKafkaStream[R](parameters: ParameterTool, clazz: Class[_ <: DeserializationSchema[R]]): DataStream[R] = {
//
//    env.getConfig.setGlobalJobParameters(parameters)
//    val props: Properties = new Properties()
//
//    props.setProperty("bootstrap.servers", parameters.getRequired("bootstrap.servers"));
//    //指定组ID
//    props.setProperty("group.id", parameters.getRequired("group.id"));
//    //如果没有记录偏移量，第一次从最开始消费
//    props.setProperty("auto.offset.reset", parameters.get("auto.offset.reset", "earliest"));
//    //kafka的消费者不自动提交偏移量
//    props.setProperty("enable.auto.commit", parameters.get("enable.auto.commit", "false"));
//
//    val topics: String = parameters.getRequired("topics")
//    val kafkaTopics: Array[String] = topics.split(",")
//    val topicsList: util.ArrayList[String] = new util.ArrayList[String]()
//
//    for (elem <- kafkaTopics) {
//      topicsList.add(elem)
//    }
//
//    val value: FlinkKafkaConsumer[R] = new FlinkKafkaConsumer[R](topics, clazz.newInstance(), props)
//    env.addSource(value)
//  }
//
//  def getStreamExecutionEnvironment: StreamExecutionEnvironment = {
//    env
//  }
}
