package flink.mystream.windows

import org.apache.flink.streaming.api.TimeCharacteristic
import org.apache.flink.streaming.api.functions.timestamps.BoundedOutOfOrdernessTimestampExtractor
import org.apache.flink.streaming.api.scala.{StreamExecutionEnvironment, _}
import org.apache.flink.streaming.api.windowing.time.Time

/**
 *测试窗口开始的时间
 * 第一条数据的时间为1612969009753 word1
 * 经过计算 窗口的开始时间为1612969008000。因为窗口的长度为7s。外加1s的延时。所以等到输入1612969016000 word3的时候就会触发窗口的运算
 * println((1612969009753L) - (1612969009753L + 7000) % 7000)
 */
case class TestData(timestamp:Long,word:String)

object T0_nc_window {
  def main(args: Array[String]): Unit = {
    val environment: StreamExecutionEnvironment = StreamExecutionEnvironment.getExecutionEnvironment
    environment.setStreamTimeCharacteristic(TimeCharacteristic.EventTime)

    environment.setParallelism(1)

    val socketStream: DataStream[String] = environment.socketTextStream("192.168.28.200", 2222)
    val windowedStream: DataStream[TestData] = socketStream
      .map((row: String) =>TestData(row.split(" ")(0).toLong,row.split(" ")(0)))
      .assignTimestampsAndWatermarks(new
          BoundedOutOfOrdernessTimestampExtractor[TestData](Time.seconds(1)) {
        override def extractTimestamp(element: TestData): Long
        = element.timestamp
      })
      .keyBy(_.word)
      .timeWindow(Time.milliseconds(7000))
      .reduce((r1,r2)=>TestData(r1.timestamp,"hello "+r2.word))
    windowedStream.print("window output is")
    socketStream.print("input data is")

    environment.execute()

  }
}
