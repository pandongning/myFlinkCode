package flink.mystream.windows.t5_trigger

import java.util.Properties

import flink.mystream.beans.SensorReading
import org.apache.flink.api.common.functions.{AggregateFunction, ReduceFunction}
import org.apache.flink.api.common.serialization.SimpleStringSchema
import org.apache.flink.api.common.state.ListStateDescriptor
import org.apache.flink.streaming.api.TimeCharacteristic
import org.apache.flink.streaming.api.functions.timestamps.BoundedOutOfOrdernessTimestampExtractor
import org.apache.flink.streaming.api.scala.StreamExecutionEnvironment
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer
import org.apache.flink.streaming.api.scala._
import org.apache.flink.streaming.api.scala.function.ProcessWindowFunction
import org.apache.flink.streaming.api.windowing.time.Time
import org.apache.flink.streaming.api.windowing.triggers.{Trigger, TriggerResult}
import org.apache.flink.streaming.api.windowing.windows.TimeWindow
import org.apache.flink.util.Collector


object T1_OnElement {

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

    val sensorReading: DataStream[SensorReading] = lineSource.map((line: String) => {
      val strings: Array[String] = line.split(",")
      SensorReading(strings(0), strings(1).trim.toLong, strings(2).trim.toDouble)
    }
    ).assignTimestampsAndWatermarks(new BoundedOutOfOrdernessTimestampExtractor[SensorReading](Time.milliseconds(1)) {
      override def extractTimestamp(element: SensorReading): Long = element.timestamp
    })


    val keyedStream: KeyedStream[SensorReading, String] = sensorReading.keyBy((_: SensorReading).id)

    keyedStream.timeWindow(Time.milliseconds(3))
      .trigger(new MyTrigger())
      .process(new T1_ProcessWindowFunction)
      .print()

    environment.execute()
  }
}

/**
 * 每遇到一个id为sensor_1的event
 * 则计算此前所有key的温度平均值
 *
 * 输入
 * sensor_1, 1599990790000,1
 * 输出
 * 5> sensor_1	1599990790002	1.0
 * 输入
 * 5> sensor_1	1599990790002	1.1
 * 输出
 * 5> sensor_1	1599990790002	1.05
 * 从上面得输出可以看出第一次process函数处理得时候，其elements里面只有一条数据
 * 当第二条数据sensor_1	1599990790002	1.1到来得时候，则elements里面只有二条数据
 */
class T1_ProcessWindowFunction extends ProcessWindowFunction[SensorReading, String, String, TimeWindow] {
  //  process只执行一次，一次处理该窗口里面的所有数据
  override def process(key: String, context: Context, elements: Iterable[SensorReading], out: Collector[String]): Unit = {
    val elementsIterator: Iterator[SensorReading] = elements.iterator

    var temperatureSum: Double = 0.0
    while (elementsIterator.hasNext) {
      temperatureSum += elementsIterator.next().temperature
    }

    out.collect(key + "\t" + context.window.getEnd + "\t" + temperatureSum / elements.size)
  }
}


class MyTrigger extends Trigger[SensorReading, TimeWindow] {

  override def onElement(element: SensorReading, timestamp: Long, window: TimeWindow, ctx: Trigger.TriggerContext): TriggerResult = {
    // 此处可以定义自己业务逻辑，按照自己的要求，决定达到什么条件的时候才触发窗口操做
//    if (element.id == "sensor_1") {
//      return TriggerResult.FIRE
//    }
//    TriggerResult.CONTINUE

    TriggerResult.FIRE   //表示对于窗口每来一条数据，则触发触发计算
  }

  override def onProcessingTime(time: Long, window: TimeWindow, ctx: Trigger.TriggerContext): TriggerResult = {
    TriggerResult.CONTINUE
  }

  override def onEventTime(time: Long, window: TimeWindow, ctx: Trigger.TriggerContext): TriggerResult = {
    TriggerResult.CONTINUE
  }

  override def clear(window: TimeWindow, ctx: Trigger.TriggerContext): Unit = {

  }
}
