package flink.mystream.operator

import org.apache.flink.api.common.functions.MapFunction
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

    //    此处为了进行演示，则10s一个ck。// 默认情况下如果不设置时间checkpoint是没有开启的
    environment.enableCheckpointing(20000L)

    /** 设置两个Checkpoint 之间最少等待时间，如设置Checkpoint之间最少是要等 500ms
     * 为了避免每隔1000ms做一次Checkpoint的时候，前一次太慢和后一次重叠到一起去了
     * 如:高速公路上，每隔1s关口放行一辆车，但是规定了两车之前的最小车距为500m
     */
    environment.getCheckpointConfig.setMinPauseBetweenCheckpoints(1000L)
    environment.getCheckpointConfig.setCheckpointingMode(CheckpointingMode.EXACTLY_ONCE)
    environment.getCheckpointConfig.setCheckpointTimeout(10000L)
    //    在执行checkpoint的过程中发生错误，判断任务是否失败。这是一个默认的行为。开启之后会忽略chenkpoint失败，继续运行任务。
    environment.getCheckpointConfig.setFailOnCheckpointingErrors(false)
    //    一个时刻只允许进行一个ck
    environment.getCheckpointConfig.setMaxConcurrentCheckpoints(1)
    //    程序停止得时候保留外部得ck
    environment.getCheckpointConfig.enableExternalizedCheckpoints(ExternalizedCheckpointCleanup.DELETE_ON_CANCELLATION)

    //    当一个任务有save point得时候，是否应该先从save point重启，还是从check point重启.
    //    下面得设置表示先从check point重启.
    environment.getCheckpointConfig.setPreferCheckpointForRecovery(true)
    //    失败之后自动重启3次，每次的间隔设置为120s。超过3次则直接判定任务失败。
    environment.setRestartStrategy(RestartStrategies.fixedDelayRestart(3, org.apache.flink.api.common.time.Time.seconds(120)))
    //    默认的超时时间就是10min
    environment.getCheckpointConfig.setCheckpointTimeout(10 * 60 * 1000)

    //    设置缓冲区的大小，只有当缓冲区里面的数据满的时候才会进行处理，这个会提高flink系统的效率，但是会导致延时
    environment.setBufferTimeout(1000L)

    //    设置CK的目录
    import org.apache.flink.runtime.state.filesystem.FsStateBackend
    environment.setStateBackend(new FsStateBackend("hdfs://LocalOne:9000/flinkCk/T0_WordCount"))


    //    设置是使用那种时间戳
    environment.setStreamTimeCharacteristic(characteristic = TimeCharacteristic.EventTime)

    //    environment.disableOperatorChaining()

    val textDstream: DataStream[String] = environment
      .socketTextStream(host, Integer.valueOf(port))
      .uid("source")

    println(org.apache.flink.streaming.api.windowing.time.Time.hours(-8).toMilliseconds)

    val value2: DataStream[Array[String]] = textDstream.map((line: String) => line.split("\\s"))

    value2.flatMap((a: Array[String]) =>a)

//    val value1: DataStream[String] = textDstream.flatMap((line: String) => line.split("\\s"))
//      .filter((word: String) => word.nonEmpty)

    val dataStream: DataStream[(String, Int)] = value1.map(new MapFunction[String, (String, Int)] {
      override def map(value: String) = {
        //        下面虽然发送了异常，但是由于flink可以自动得重启。所以此异常不会影响作业得正常执行
        if (value.equals("hello")) {
          println(1 / 0)
          throw new ArithmeticException("除数不能是0")
        }
        (value, 1)
      }
    })
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
