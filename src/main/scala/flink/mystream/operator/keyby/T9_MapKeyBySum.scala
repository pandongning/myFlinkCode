package flink.mystream.operator.keyby

import org.apache.flink.api.scala._
import org.apache.flink.streaming.api.scala.{DataStream, KeyedStream, StreamExecutionEnvironment}

object MapKeyBySum {
  def main(args: Array[String]): Unit = {
    val environment: StreamExecutionEnvironment = StreamExecutionEnvironment.getExecutionEnvironment

    val inputStream: DataStream[String] = environment.readTextFile("src/main/resources/sensor.txt")

    val mapStream: DataStream[SensorReading] = inputStream.map(
      (item: String) => {
        val items: Array[String] = item.split(",")
        SensorReading(items(0).trim, items(1).trim.toLong, items(2).trim.toDouble)
      }
    )

    //    注意最好将流里面每个元素的类型变为case class，则方便取值
    //    下面的第一个泛型表示流里面每个event的类型，第二个泛型表示key的类型
    val keyStream: KeyedStream[SensorReading, String] = mapStream.keyBy((sensorReading: SensorReading) => sensorReading.id)

    //    如果直接指定key的position 则得到的流的key为一个Tuple
//    val keyStreamTwo: KeyedStream[SensorReading, Tuple] = mapStream.keyBy(0)

    //    sum操做的时候最好直接指定字段，则利用后期的维护,见名知道其意思
//    keyStream.sum(2).print()
    keyStream.sum("temperature").print()


    environment.execute("MTest")

  }
}

// 定义传感器数据样例类
case class SensorReading(id: String, timestamp: Long, temperature: Double)