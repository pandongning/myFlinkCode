package flink.mystream.windows.t7_lateness

import java.util.Properties

import flink.mystream.beans.SensorReading
import org.apache.flink.api.common.serialization.SimpleStringSchema
import org.apache.flink.streaming.api.TimeCharacteristic
import org.apache.flink.streaming.api.functions.timestamps.BoundedOutOfOrdernessTimestampExtractor
import org.apache.flink.streaming.api.scala.function.ProcessWindowFunction
import org.apache.flink.streaming.api.scala.{StreamExecutionEnvironment, _}
import org.apache.flink.streaming.api.windowing.time.Time
import org.apache.flink.streaming.api.windowing.windows.TimeWindow
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer
import org.apache.flink.util.Collector

object t7_lateness {

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
    ).assignTimestampsAndWatermarks(new BoundedOutOfOrdernessTimestampExtractor[SensorReading](Time.milliseconds(1000)) {
      override def extractTimestamp(element: SensorReading): Long = element.timestamp
    })


    val keyedStream: KeyedStream[SensorReading, String] = sensorReading.keyBy(_.id)

    /**
     * 输入
     * >sensor_1, 1599990791000,1
     * >sensor_1, 1599990791000,1.1
     * >sensor_1, 1599990791000,1.2
     * >sensor_2, 1599990792000,2
     * >sensor_2, 1599990792000,2.1
     * >sensor_2, 1599990792000,2.2
     * >sensor_1, 1599990793000,3
     * >sensor_1, 1599990793000,3.1
     * >sensor_1, 1599990793000,3.2
     * 输入上面的记录
     * 则此时会输出下面的信息
     * 5> sensor_1	1599990792000	3.3
     * 同时说明了窗口的结束时间是1599990792000
     * 因为窗口的开始时间经过计算为1599990790000。窗口的长度为3.则应该窗口的结束时间为1599990792000
     * 因为水印的产生会比流里面记录的最大时间戳晚1000ms。所以等到输入时间戳为sensor_1, 1599990793000,3的记录时才会触发窗口的计算
     *
     *
     * 此时虽然已经触发了窗口的计算，但是我们设置的延时为2000ms
     * 所以再次输入
     * sensor_1, 1599990791000,4
     * 此时会输出
     * 5> sensor_1	1599990792000	7.3
     * 即迟到的数据会再次触发窗口的计算。
     * 注意此时输出的结果应该用来更新前面输出的结果
     *
     * 再输入sensor_1, 1599990791000,4.1
     * 同理对于延时的数据再次输出
     * sensor_1	1599990792000	11.399999999999999
     *
     *
     *
     * 因为延时是2s。外加水印产生的慢1s。所以总共的延时应该为3s
     * 下面则验证，如果超过了延时的最大时间，则晚来的数据，不会触发窗口的计算。即其会被抛弃
     *
     *再此输入
     * >sensor_1, 1599990794000,2
     * >sensor_1, 1599990790000,1
     * 对应的输出如下.即还会触发窗口的操作
     * 5> sensor_1	1599990792000	12.5
     * 再次输入。即每个分区都得时间戳都超过1599990794000
     * >sensor_1, 1599990794000,2
     * >sensor_1, 1599990794000,2
     * 再次输入
     * >sensor_1, 1599990790000,1
     * 发现迟到得数据依然可以触发窗口操作
     * 5> sensor_1	1599990792000	13.5
     *
     *
     * 再次输入
     * >sensor_1, 1599990795000,1
     * >sensor_1, 1599990790000,1
     * 输出，发现迟到得数据依然可以触发窗口操作
     * 5> sensor_1	1599990792000	14.5
     *
     *
     *再次输入1599990795000
     * >sensor_1, 1599990795000,1
     * >sensor_1, 1599990795000,1
     * >sensor_1, 1599990790000,1
     * 发现迟到得数据依然可以触发
     * sensor_1	1599990792000	15.5
     *
     *再次输入1599990796000
     * >sensor_1, 1599990796000,1
     * >sensor_1, 1599990796000,1
     * >sensor_1, 1599990790000,1
     * 仍然可以触发
     * 5> sensor_1	1599990792000	16.5
     *
     *输入
     * >sensor_1, 1599990796000,1
     * >sensor_1, 1599990790000,1
     * 输出
     * 5>sensor_1	1599990792000	17.5
     *
     * 所以此时可以看出其是在1599990796000以后。延时的数据才不会触发窗口的操作的。
     * 因为窗口的结束时间为1599990792000+1000水印延时+2000ms延时=1599990795000
     * 同时可以看出延时的数据，仍然可以得到一个结果，
     * 所以此时对于延时数据触发得到的结果，应该用于更新前面的结果
     *
     *
     * 当输入
     * >sensor_1, 1599990797000,1
     * >sensor_1, 1599990797000,1
     * >sensor_1, 1599990797000,1
     * 则此时的输出如下
     * 2> sensor_2	1599990795000	6.3
     * 5> sensor_1	1599990795000	15.3
     *
     *再次输入
     * >sensor_1, 1599990790000,1
     * 发现其已经被丢弃，不会触发窗口的操作
     *
     *
     * 总共的输入流程如下
     * >sensor_1, 1599990791000,1
     * >sensor_1, 1599990791000,1.1
     * >sensor_1, 1599990791000,1.2
     * >sensor_2, 1599990792000,2
     * >sensor_2, 1599990792000,2.1
     * >sensor_2, 1599990792000,2.2
     * >sensor_1, 1599990793000,3
     * >sensor_1, 1599990793000,3.1
     * >sensor_1, 1599990793000,3.2
     * >sensor_1, 1599990791000,4.1
     * >sensor_1, 1599990791000,4.1
     * >sensor_1, 1599990794000,2
     * >sensor_1, 1599990790000,1
     * >sensor_1, 1599990794000,2
     * >sensor_1, 1599990794000,2
     * >sensor_1, 1599990790000,1
     * >sensor_1, 1599990795000,1
     * >sensor_1, 1599990790000,1
     * >sensor_1, 1599990795000,1
     * >sensor_1, 1599990795000,1
     * >sensor_1, 1599990790000,1
     * >sensor_1, 1599990796000,1
     * >sensor_1, 1599990796000,1
     * >sensor_1, 1599990790000,1
     * >sensor_1, 1599990796000,1
     * >sensor_1, 1599990790000,1
     * >sensor_1, 1599990797000,1
     * >sensor_1, 1599990797000,1
     * >sensor_1, 1599990797000,1
     * >sensor_1, 1599990790000,1
     */

    keyedStream.timeWindow(Time.milliseconds(3000))
      .allowedLateness(Time.milliseconds(2000))
      .process(new T7_ProcessWindowFunction)
      .print()

    environment.execute()
  }
}

class T7_ProcessWindowFunction extends ProcessWindowFunction[SensorReading, String, String, TimeWindow] {

  override def process(key: String, context: Context, elements: Iterable[SensorReading], out: Collector[String]): Unit = {
    val elementsIterator: Iterator[SensorReading] = elements.iterator

    var sum: Double = 0

    while (elementsIterator.hasNext) {
      val sensorReading: SensorReading = elementsIterator.next()
      sum += sensorReading.temperature
    }

    out.collect(key + "\t" + context.window.getEnd + "\t" + sum)
  }
}
