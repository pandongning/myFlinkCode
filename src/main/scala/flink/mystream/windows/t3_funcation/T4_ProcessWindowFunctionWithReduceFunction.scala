package flink.mystream.windows.t3_funcation

import java.util.Properties

import flink.mystream.beans.SensorReading
import org.apache.flink.api.common.functions.ReduceFunction
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

/**
 * 上面的ProcessWindowFunction 十分的浪费资源，因为其需要将一个窗口里面的元素全部缓存，所以如果窗口的size过大，就会导致oom
 * 所以可以将ProcessWindowFunction和ReduceFunction, an AggregateFunction之类的聚合函数结合。
 * 其先使用ReduceFunction, an AggregateFunction对于窗口里面的元素增量的聚合，
 * 然后在窗口关闭的时候，将聚合的结果传递给ProcessWindowFunction处理。
 *
 * 此处代码是ProcessWindowFunction和 ReduceFunction结合
 */

object T4_ProcessWindowFunctionWithReduceFunction {

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

    keyedStream
      .timeWindow(Time.milliseconds(5))
      //第一个函数对数据进行增量的聚合，然后将聚合的结果，发送给第二个函数统一做最后的处理
      .reduce(new MyReduceFunction(),new T4_MyProcessWindowFunction)
      .print()

    environment.execute()
  }
}

// 将窗口里面的传感器维度，增量的聚合。最后聚合结果为SensorReading，但是其温度已经是此key对应的窗口里面的所有event的温度之和
class MyReduceFunction extends ReduceFunction[SensorReading] {
  override def reduce(value1: SensorReading, value2: SensorReading): SensorReading = {
    SensorReading(value1.id, value1.timestamp, value1.temperature + value2.temperature)
  }
}

// 接受MyReduceFunction的结果，然后将其变为字符串输出即可
class T4_MyProcessWindowFunction extends ProcessWindowFunction[SensorReading, String, String, TimeWindow] {
  override def process(key: String, context: Context, elements: Iterable[SensorReading], out: Collector[String]): Unit = {
    val list: List[SensorReading] = elements.toList
    val str: String = list.mkString("$")
    out.collect(str)
  }
}
