package flink.mystream.windows.t3_funcation

import java.util.Properties

import flink.mystream.beans.SensorReading
import org.apache.flink.api.common.serialization.SimpleStringSchema
import org.apache.flink.streaming.api.TimeCharacteristic
import org.apache.flink.streaming.api.functions.timestamps.BoundedOutOfOrdernessTimestampExtractor
import org.apache.flink.streaming.api.scala.StreamExecutionEnvironment
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer
import org.apache.flink.streaming.api.scala._
import org.apache.flink.streaming.api.scala.function.ProcessWindowFunction
import org.apache.flink.streaming.api.windowing.time.Time
import org.apache.flink.streaming.api.windowing.windows.TimeWindow
import org.apache.flink.util.Collector

object T3_ProcessWindowFunction {
  def main(args: Array[String]): Unit = {

    val environment: StreamExecutionEnvironment = StreamExecutionEnvironment.getExecutionEnvironment
    environment.setStreamTimeCharacteristic(TimeCharacteristic.EventTime)

    val properties = new Properties()
    properties.setProperty("bootstrap.servers", "LocalOne:9092")
    properties.setProperty("group.id", "pdn2")
    properties.setProperty("key.deserializer", "org.apache.kafka.common.serialization.StringDeserializer")
    properties.setProperty("value.deserializer", "org.apache.kafka.common.serialization.StringDeserializer")

    val flinkKafkaConsumer: FlinkKafkaConsumer[String] = new FlinkKafkaConsumer[String]("first", new SimpleStringSchema(), properties)
    flinkKafkaConsumer.setStartFromLatest()


    val lineSource: DataStream[String] = environment.addSource(flinkKafkaConsumer)

    val sensorReading: DataStream[SensorReading] = lineSource.map(line => {
      val strings: Array[String] = line.split(",")
      SensorReading(strings(0), strings(1).trim.toLong, strings(2).trim.toDouble)
    }
    ).assignTimestampsAndWatermarks(new BoundedOutOfOrdernessTimestampExtractor[SensorReading](Time.milliseconds(1)) {
      override def extractTimestamp(element: SensorReading): Long = element.timestamp
    })


    val keyedStream: KeyedStream[SensorReading, String] = sensorReading.keyBy(_.id)

    keyedStream.timeWindow(Time.milliseconds(3))
      .process(new MyProcessWindowFunction()).print()

    environment.execute()
  }
}

/**
 * IN     The type of the input value.
 * OUT    The type of the output value.
 * KEY    The type of the key.
 *        其就是流进行keyBy的时候key的类型，Note The key parameter is the key that is extracted via the KeySelector that was specified for the keyBy() invocation.
 *        In case of tuple-index keys or string-field references this key type is always Tuple and you have to manually cast it to a tuple of the correct size to extract the key fields.
 * W      The type of the window.
 */

class MyProcessWindowFunction extends ProcessWindowFunction[SensorReading, Int, String, TimeWindow] {

  //  参数context可以用于获取状态信息---a Context object with access to time and state information
  //统计窗口内event的总数
  override def process(key: String, context: Context, elements: Iterable[SensorReading], out: Collector[Int]): Unit = {
    var count: Int = 0

    for (elem <- elements) {
      count += 1
    }

    // out参数用于将处理的结果发送给下一个算子
    out.collect(count)
  }
}