package flink.mystream.windows.t8_sideOutputLateData

import java.util.Properties

import flink.mystream.beans.SensorReading
import org.apache.flink.api.common.serialization.SimpleStringSchema
import org.apache.flink.streaming.api.TimeCharacteristic
import org.apache.flink.streaming.api.functions.timestamps.BoundedOutOfOrdernessTimestampExtractor
import org.apache.flink.streaming.api.scala.{StreamExecutionEnvironment, _}
import org.apache.flink.streaming.api.windowing.time.Time
import org.apache.flink.streaming.api.windowing.windows.TimeWindow
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer

object SideOutputLateData {

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
    ).assignTimestampsAndWatermarks(new BoundedOutOfOrdernessTimestampExtractor[SensorReading](Time.milliseconds(1000)) {
      override def extractTimestamp(element: SensorReading): Long = element.timestamp
    })

    val keyedStream: KeyedStream[SensorReading, String] = sensorReading.keyBy((_: SensorReading).id)

    val lateOutputTag: OutputTag[SensorReading] = new OutputTag[SensorReading]("lateData")

    val value: WindowedStream[SensorReading, String, TimeWindow] = keyedStream
      .timeWindow(Time.milliseconds(3000))
      .allowedLateness(Time.milliseconds(2000))
      .sideOutputLateData(lateOutputTag)


    val result: DataStream[SensorReading] = value
      .reduce((sensorReadingAgg: SensorReading, sensorReadingElement: SensorReading) => {
        SensorReading(sensorReadingAgg.id, sensorReadingAgg.timestamp, sensorReadingAgg.temperature + sensorReadingElement.temperature)
      })

    result.print()

    //   收集迟到的数据，迟到的数据又会组成一个DataStream，此时则可以根据自己的业务逻辑进行处理
    val lateDate: DataStream[SensorReading] = result.getSideOutput(lateOutputTag)

    /**
     * 输入
     * >sensor_1, 1599990791000,1
     * >sensor_1, 1599990791000,1.1
     * >sensor_1, 1599990791000,1.2
     * >sensor_2, 1599990791000,1
     * >sensor_2, 1599990791000,2
     * >sensor_2, 1599990791000,3
     * >sensor_2, 1599990792000,4
     * >sensor_2, 1599990792000,5
     * >sensor_2, 1599990792000,6
     * >sensor_1, 1599990793000,3
     * >sensor_2, 1599990793000,3
     * >sensor_2, 1599990793000,3
     * 输出
     * 5> SensorReading(sensor_1,1599990791000,3.3)
     * 2> SensorReading(sensor_2,1599990791000,6.0)
     *
     * 可以看出上面输入的1599990792000和1599990793000还没有被触发
     * 因为按照时间窗口为3s外加1s的水印延时，此时对于1599990792000开始的窗口的结束时间应该为1599990795000
     *
     * 输入下面的，一直到输入的时间为1599990796000。才会再次触发窗口操作
     * 输入
     * >sensor_1, 1599990794000,4
     * >sensor_1, 1599990794000,4
     * >sensor_1, 1599990794000,4
     * >sensor_1, 1599990795000,1
     * >sensor_2, 1599990795000,1
     * >sensor_2, 1599990795000,1
     * >sensor_1, 1599990796000,1
     * >sensor_1, 1599990796000,1
     * >sensor_1, 1599990796000,1
     *
     * 输出
     * 2> SensorReading(sensor_2,1599990792000,21.0)
     * 5> SensorReading(sensor_1,1599990793000,15.0)
     *
     *
     * 最后如果再次输入
     * sensor_1, 1599990791000,1
     * 则显示其为迟到的数据
     * lateDate:6> SensorReading(>sensor_1,1599990791000,1.0)
     */
    lateDate.print("lateDate")

    environment.execute()

  }
}
