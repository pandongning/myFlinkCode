package flink.mystream.ck

import java.lang
import java.util.Properties

import flink.mystream.beans.SensorReading
import org.apache.flink.api.common.functions.{RichFlatMapFunction, RichMapFunction}
import org.apache.flink.api.common.restartstrategy.RestartStrategies
import org.apache.flink.api.common.serialization.SimpleStringSchema
import org.apache.flink.api.common.state.{ValueState, ValueStateDescriptor}
import org.apache.flink.configuration.Configuration
import org.apache.flink.contrib.streaming.state.RocksDBStateBackend
import org.apache.flink.streaming.api.environment.CheckpointConfig.ExternalizedCheckpointCleanup
import org.apache.flink.streaming.api.{CheckpointingMode, TimeCharacteristic}
import org.apache.flink.streaming.api.functions.KeyedProcessFunction
import org.apache.flink.streaming.api.functions.timestamps.BoundedOutOfOrdernessTimestampExtractor
import org.apache.flink.streaming.api.scala._
import org.apache.flink.streaming.api.windowing.time.Time
import org.apache.flink.streaming.connectors.kafka.{FlinkKafkaProducer, KafkaSerializationSchema}
import org.apache.flink.util.Collector
import org.apache.kafka.clients.producer.ProducerRecord


object ProcessFunctionTest {

  def main(args: Array[String]): Unit = {
    System.setProperty("hadoop.home.dir", "C:\\Users\\pdn\\mySoft\\hadoop-2.7.2")
    System.setProperty("HADOOP_USER_NAME", "root")

    val env: StreamExecutionEnvironment = StreamExecutionEnvironment.getExecutionEnvironment

    env.setParallelism(1)

    //    Flink默认不是来一条数据就处理一条数据。
    //而是为了提高吞吐量，先将来的数据存储起来，放在缓存里面然后再发送处理
    //但是如果缓存一直存储不满，则不会发送。
    //所以可以设置缓存的超时时间，超过这个时间，则不管缓存是否满了，都会发送缓存里面的数据。
    env.setBufferTimeout(3000)

    //    设置使用eventTime
    env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime)

    //    配置ck的信息
    env.enableCheckpointing(50000)
    env.getCheckpointConfig.setCheckpointingMode(CheckpointingMode.EXACTLY_ONCE)

    //    因为Checkpoint的时候需要将状态信息保存到文件系统，或者是其他的存储文件里面。
    //    所以其牵扯到IO的操做，所以此时可能出现超时的问题，如果ck的时候超过下面指定的时间，则抛弃
    //    当前的ck。到下次时间间隔的时候再进行ck操做
    env.getCheckpointConfig.setCheckpointTimeout(100000)

    //    false表示，ck失败，不会停止整个job。下面过期是建议将其配置到全局的文件里面
    env.getCheckpointConfig.setFailOnCheckpointingErrors(false)

    //    最大同时进行的ck数量，如果ck的间隔小于，超时的时间，则会导致，一个ck还未结束 另一个ck已经开启执行
    env.getCheckpointConfig.setMaxConcurrentCheckpoints(1)
    //    两个ck之间最小的时间间隔，如何设置它，则不能设置上面的setMaxConcurrentCheckpoints
    //    env.getCheckpointConfig.setMinPauseBetweenCheckpoints(100)

    //  job失败时候，是否自动的删除外部持久化的ck信息。下面的配置表示删除
    env.getCheckpointConfig.enableExternalizedCheckpoints(ExternalizedCheckpointCleanup.DELETE_ON_CANCELLATION)

    //    设置失败重启的策略
    //    300秒之内，重启最多3次，每次重启之间的时间间隔为10秒。如果3次之后任务还继续失败，则判定该job失败
    env.setRestartStrategy(RestartStrategies.failureRateRestart(3, org.apache.flink.api.common.time.Time.seconds(300), org.apache.flink.api.common.time.Time.seconds(10)))

    //    失败之后自动重启3次，每次的间隔设置为120s
    //    env.setRestartStrategy(RestartStrategies.fixedDelayRestart(3, org.apache.flink.api.common.time.Time.seconds(120)))

    //    设置ck保存的目录。此处是设置的状态后端为RocksDB.
    //    方法过时的原因是目前建议在全局配置里面配置，而不是在代码里面单独配置
    env.setStateBackend(new RocksDBStateBackend("hdfs://LocalOne:9000/flink"))


    val stream: DataStream[String] = env.socketTextStream("LocalOne", 7777)


    val dataStream: DataStream[SensorReading] = stream.map((data: String) => {
      val dataArray: Array[String] = data.split(",")
      SensorReading(dataArray(0).trim, dataArray(1).trim.toLong, dataArray(2).trim.toDouble)
    })
      .assignTimestampsAndWatermarks(new BoundedOutOfOrdernessTimestampExtractor[SensorReading](Time.seconds(1)) {
        override def extractTimestamp(element: SensorReading): Long = element.timestamp * 1000
      })

    //需求：监控温度传感器的温度值，如果温度值在一秒钟之内(processing time)连续上升， 则报警
    val processedStream: DataStream[String] = dataStream.keyBy(_.id)
      .process(new TempIncreAlert())


    //    val processedStream2 = dataStream.keyBy(_.id)
    //      .process(new TempChangeAlert2(10.0))
    //      .flatMap(new TempChangeAlert(10.0))  //此处调用flatMap实现和上面process一样的效果


    //    第一个泛型(String, Double, Double)是输出的元素类型。第二个参数是状态的类型
    /**
     * val processedStream3 = dataStream.keyBy(_.id)
     * .flatMapWithState[(String, Double, Double), Double] {
     * // 如果没有状态的话，也就是没有数据来过，那么就将当前数据温度值存入状态
     * case (input: SensorReading, None) => (List.empty, Some(input.temperature))
     * // 如果有状态，就应该与上次的温度值比较差值，如果大于阈值就输出报警
     * case (input: SensorReading, lastTemp: Some[Double]) =>
     * val diff = (input.temperature - lastTemp.get).abs
     * if (diff > 10.0) {
     * (List((input.id, lastTemp.get, input.temperature)), Some(input.temperature))
     * } else
     * (List.empty, Some(input.temperature))
     * }
     */


    val properties: Properties = new Properties()
    properties.setProperty("bootstrap.servers", "LocalTwo:9092")
    properties.setProperty("group.id", "pdn2")
    properties.setProperty("key.deserializer", "org.apache.kafka.common.serialization.StringDeserializer")
    properties.setProperty("value.deserializer", "org.apache.kafka.common.serialization.StringDeserializer")
    properties.setProperty("transaction.timeout.ms", 1000 * 60 * 65 + "")


    val flinkKafkaProducer011: FlinkKafkaProducer[String] = new FlinkKafkaProducer[String]("flinkSink", new SimpleStringSchema(), properties)
    val flinkKafkaProducer: FlinkKafkaProducer[String] = new FlinkKafkaProducer[String]("flinkSink", new SimpleStringSchema(), properties)

    stream.addSink(flinkKafkaProducer011)


    //    dataStream.print("input data")
    stream.print()

    env.execute("process function test")
  }
}


/**
 * 5秒之内温度连续上升，则触发定时器报警
 */
class TempIncreAlert() extends KeyedProcessFunction[String, SensorReading, String] {


  override def open(parameters: Configuration): Unit = {

  }

  override def close(): Unit = super.close()

  // 定义一个状态，用来保存上一个数据的温度值
  lazy val lastTemp: ValueState[Double] = getRuntimeContext.getState(new ValueStateDescriptor[Double]("lastTemp", classOf[Double]))
  // 定义一个状态，用来保存定时器的时间戳
  lazy val currentTimer: ValueState[Long] = getRuntimeContext.getState(new ValueStateDescriptor[Long]("currentTimer", classOf[Long]))

  //  value为当前处理的元素
  override def processElement(value: SensorReading, ctx: KeyedProcessFunction[String, SensorReading, String]#Context, out: Collector[String]): Unit = {
    // 先取出上一个温度值
    val preTemp: Double = lastTemp.value()
    // 更新温度值
    lastTemp.update(value.temperature)
    //取出定时器的时间戳
    val curTimerTs: Long = currentTimer.value()

    if (value.temperature < preTemp || preTemp == 0.0) {
      // 如果温度下降，或是第一条数据，删除上一条记录的定时器并清空状态
      ctx.timerService().deleteProcessingTimeTimer(curTimerTs)
      currentTimer.clear() //清空状态
    } else if (value.temperature > preTemp && curTimerTs == 0) {
      // 温度上升且没有设过定时器，则注册定时器
      //      注意此处使用的是ProcessingTime.
      val timerTs: Long = ctx.timerService().currentProcessingTime() + 5000L
      ctx.timerService().registerProcessingTimeTimer(timerTs)

      //      表示使用的是eventTime.下面的两句是一对
      //      val timerTs: Long =ctx.timerService().currentWatermark()+5000L
      //      ctx.timerService().registerEventTimeTimer(timerTs)

      //更新定时器的时间戳
      currentTimer.update(timerTs)
    }

  }

  override def onTimer(timestamp: Long, ctx: KeyedProcessFunction[String, SensorReading, String]#OnTimerContext, out: Collector[String]): Unit = {
    // 输出报警信息
    out.collect(ctx.getCurrentKey + "温度连续上升")
    //    输出报警信息之后必须清楚状态
    currentTimer.clear()
  }
}


//其和下面的TempChangeAlert2功能一样，只是另一个实现
//因为其继承是RichFlatMapFunction，所以其里面也可以获得状态信息，所以此处表示rich函数里面也有状态信息
class TempChangeAlert(threshold: Double) extends RichFlatMapFunction[SensorReading, (String, Double, Double)] {

  private var lastTempState: ValueState[Double] = _

  override def open(parameters: Configuration): Unit = {
    // 初始化的时候声明state变量
    lastTempState = getRuntimeContext.getState(new ValueStateDescriptor[Double]("lastTemp", classOf[Double]))
  }

  override def flatMap(value: SensorReading, out: Collector[(String, Double, Double)]): Unit = {
    // 获取上次的温度值
    val lastTemp = lastTempState.value()
    // 用当前的温度值和上次的求差，如果大于阈值，输出报警信息
    val diff = (value.temperature - lastTemp).abs
    if (diff > threshold) {
      out.collect((value.id, lastTemp, value.temperature))
    }
    lastTempState.update(value.temperature)
  }

}


//第一个泛型是key的类型。第二个是输出的流里面每个event的类型。第三个泛型是输出的泛型类型
//此处的需求是两条从传感器传递过来的event的温度只差大于10则报警，报警信息是返回(传感器id,第一条记录的温度，第二条记录的温度)
class TempChangeAlert2(threshold: Double) extends KeyedProcessFunction[String, SensorReading, (String, Double, Double)] {

  // 定义一个状态变量，保存上次的温度值
  lazy val lastTempState: ValueState[Double] = getRuntimeContext.getState(new ValueStateDescriptor[Double]("lastTemp", classOf[Double]))

  override def processElement(value: SensorReading, ctx: KeyedProcessFunction[String, SensorReading, (String, Double, Double)]#Context, out: Collector[(String, Double, Double)]): Unit = {
    // 获取上次的温度值
    val lastTemp = lastTempState.value()

    // 用当前的温度值和上次的求差，如果大于阈值，输出报警信息
    val diff = (value.temperature - lastTemp).abs
    if (diff > threshold) {
      out.collect((value.id, lastTemp, value.temperature))
    }

    //    更新温度
    lastTempState.update(value.temperature)
  }
}

//class SensorReadingKafkaSerializationSchema extends KafkaSerializationSchema[SensorReading]{
//  override def serialize(element: SensorReading, timestamp: lang.Long): ProducerRecord[Array[Byte], Array[Byte]] = {
//    new ProducerRecord("aa".getBytes(),element)
//  }
//}