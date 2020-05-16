package flink.mystream.windows.t3_funcation


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
import org.apache.flink.streaming.api.windowing.windows.TimeWindow
import org.apache.flink.util.Collector

object T5_ProcessWindowFunctionWithAggregateFunction {

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

    keyedStream.timeWindow(Time.milliseconds(5))
      .aggregate(new T5_MyAggregateFunction, new T5_ProcessWindowFunction)
      .print()

    environment.execute()

  }
}

// 得到一个窗口里面所有传感器的平均温度
class T5_MyAggregateFunction extends AggregateFunction[SensorReading, (Long, Long), Double] {

  override def createAccumulator(): (Long, Long) = (0L, 0L)

  override def add(value: SensorReading, accumulator: (Long, Long)): (Long, Long) = {
    (accumulator._1 + value.temperature.toLong, accumulator._2 + 1L)
  }

  override def getResult(accumulator: (Long, Long)): Double = {
    accumulator._1 / accumulator._2
  }

  override def merge(a: (Long, Long), b: (Long, Long)): (Long, Long) = {
    (a._1 + b._1, a._2 + b._2)
  }
}

// 接受T5_MyAggregateFunction的输出，所以此时其elements里面的全部数据，不再是窗口里面所有的全部原始event
// 而是T5_MyAggregateFunction的输出聚合输出的所有数据
// 向外输出key值+窗口的末尾时间+窗口里面所有传感器的平均温度
class T5_ProcessWindowFunction extends ProcessWindowFunction[Double, (String, Double), String, TimeWindow] {

  override def process(key: String, context: Context, elements: Iterable[Double], out: Collector[(String, Double)]): Unit = {
    context.windowState.getListState(new ListStateDescriptor[String]("name",classOf[String]))

    val end: Long = context.window.getEnd
    out.collect((key + "\t" + end, elements.head))
  }
}