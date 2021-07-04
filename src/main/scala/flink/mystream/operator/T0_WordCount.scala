package flink.mystream.operator

import org.apache.flink.api.common.restartstrategy.RestartStrategies
import org.apache.flink.api.java.utils.ParameterTool
import org.apache.flink.streaming.api.environment.CheckpointConfig.ExternalizedCheckpointCleanup
import org.apache.flink.streaming.api.scala.{DataStream, StreamExecutionEnvironment, _}
import org.apache.flink.streaming.api.{CheckpointingMode, TimeCharacteristic}

object T0_WordCount {

  def main(args: Array[String]): Unit = {
    System.setProperty("HAOOP_HOME", "C:/Users/pdn/mySoft/hadoop-2.7.2")

    val parameter: ParameterTool = ParameterTool.fromArgs(args)
    val host: String = parameter.get("host")
    val port: String = parameter.get("port")

    val environment: StreamExecutionEnvironment = StreamExecutionEnvironment.getExecutionEnvironment

    environment.enableCheckpointing(5000L)
    environment.getCheckpointConfig.setCheckpointingMode(CheckpointingMode.EXACTLY_ONCE)
    environment.getCheckpointConfig.setCheckpointTimeout(10000L)
    environment.getCheckpointConfig.setFailOnCheckpointingErrors(false)
    environment.getCheckpointConfig.setMaxConcurrentCheckpoints(1)
    environment.getCheckpointConfig.enableExternalizedCheckpoints(ExternalizedCheckpointCleanup.DELETE_ON_CANCELLATION)
    //    失败之后自动重启3次，每次的间隔设置为120s。超过3次则直接判定任务失败。
    environment.setRestartStrategy(RestartStrategies.fixedDelayRestart(3, org.apache.flink.api.common.time.Time.seconds(120)))

    //    设置缓冲区的大小，只有当缓冲区里面的数据满的时候才会进行处理，这个会提高flink系统的效率，但是会导致延时
        environment.setBufferTimeout(1000L)

//    设置CK的目录
    import org.apache.flink.runtime.state.filesystem.FsStateBackend
//    environment.setStateBackend(new FsStateBackend("hdfs://LocalOne:8020/flinkCk"))


    //    设置是使用那种时间戳
        environment.setStreamTimeCharacteristic(characteristic = TimeCharacteristic.EventTime)

    //    environment.disableOperatorChaining()

    val textDstream: DataStream[String] = environment
      .socketTextStream(host, Integer.valueOf(port))
      .uid("source")

    println(org.apache.flink.streaming.api.windowing.time.Time.hours(-8).toMilliseconds)

    val value1: DataStream[String] = textDstream.flatMap((line: String) => line.split("\\s"))
      .filter((word: String) => word.nonEmpty)

    val dataStream: DataStream[(String, Int)] = value1.map(((_: String), 1))

    //   第二个泛型参数Tuple表示key的类型
    val keyedStream: KeyedStream[(String, Int), String] = dataStream.keyBy((_: (String, Int))._1)


    val value: DataStream[(String, Int)] = keyedStream.sum(1)

    //另外一种实现的方式
    //    val dataStream: DataStream[(String, Int)] = textDstream.flatMap(line => line.split("\\s"))
    //      .filter(word => word.nonEmpty).disableChaining()
    //      .map(word => (word, 1)).keyBy(0).reduce((tupleOne, tupleTwo) => (tupleOne._1, tupleOne._2 + tupleTwo._2))

    value.print().setParallelism(1)


    environment.execute()
  }
}
