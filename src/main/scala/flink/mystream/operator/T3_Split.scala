package flink.mystream.operator


import flink.mystream.operator.keyby.SensorReading
import org.apache.flink.streaming.api.scala.{DataStream, SplitStream, StreamExecutionEnvironment}

object T3_Split {

  def main(args: Array[String]): Unit = {
    val environment: StreamExecutionEnvironment = StreamExecutionEnvironment.getExecutionEnvironment

    val inputStream: DataStream[String] = environment.readTextFile("src/main/resources/sensor.txt")

    import org.apache.flink.api.scala._

    val mapStream: DataStream[SensorReading] = inputStream.map(
      (item: String) => {
        val items: Array[String] = item.split(",")
        SensorReading(items(0).trim, items(1).trim.toLong, items(2).trim.toDouble)
      }
    )



    val splitStream: SplitStream[SensorReading] = mapStream.split((sensorReading: SensorReading) => {
      if (sensorReading.temperature < 1)
        Seq("low") //注意此处返回的是一个集合，因为一个事件可以属于多个标签。比如事件a即可以属于标签低温，也可以属于中等温度，根据自己的业务规则而定
      else if (sensorReading.temperature < 3)
        Seq("middle")
      else
        Seq("high")
    })

    val highTempStream: DataStream[SensorReading] = splitStream.select("high")
    val lowTempStream: DataStream[SensorReading] = splitStream.select("low")
    val middleTempStream: DataStream[SensorReading] = splitStream.select("middle")

    val allTempStream: DataStream[SensorReading] = splitStream.select("high", "low")

    //    highTempStream.print("high")
//    lowTempStream.print()
    middleTempStream.print()
    //    allTempStream.print()

    environment.execute("T3_Split")

  }
}
