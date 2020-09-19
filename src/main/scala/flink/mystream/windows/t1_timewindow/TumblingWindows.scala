package flink.mystream.windows.t1_timewindow

import org.apache.flink.api.scala._
import org.apache.flink.streaming.api.TimeCharacteristic
import org.apache.flink.streaming.api.scala.{DataStream, StreamExecutionEnvironment}
import org.apache.flink.streaming.api.windowing.time.Time

object TumblingWindows {
  def main(args: Array[String]): Unit = {
    import org.apache.flink.streaming.api.windowing.assigners.TumblingEventTimeWindows
    val environment: StreamExecutionEnvironment = StreamExecutionEnvironment.getExecutionEnvironment

    val text: DataStream[String] = environment.socketTextStream("LocalOne", 8888)

    environment.setStreamTimeCharacteristic(TimeCharacteristic.ProcessingTime)

    val words = text.flatMap(_.split(","))
    val value: DataStream[(String, Int)] = words.map((_, 1))

    text.flatMap(_.split(","))
      .map((_, 1))
      .keyBy(0)
      .timeWindow(Time.seconds(5))

      /**
       * 其和上面的 .timeWindow(Time.seconds(5))功能一样，但是其可以加入offset的值
       * 比如
       * .window(TumblingEventTimeWindows.of(Time.hours(1))表示统计8点到9点这个区间的
       * 但是
       * .window(TumblingEventTimeWindows.of(Time.hours(1),Time.minutes(5)))则表示统计8.05到9.05之间的数据
       *
       * 同时因为flink底层在计算时间的时候，如果我在中国使用flink，则其会获取本地的北京时间，但是其然后按照 UTC±00:00
       * 将本地北京时间转换为标准时间
       * 所以如果窗口的size为day  。此时如过不指定偏移量，因为中国时间比标准时间大8h。所以此时应该减去8h。则会得到今天0点到24点之见的数据。
       * 如果不减去8h，则会得到今天早晨8点到明天早晨8点之间的数据
       * * 则应该.window(TumblingEventTimeWindows.of(Time.hours(1),Time.minutes(5)))
       */

      .sum(1)
      .print()
      .setParallelism(1)

    environment.execute("TumblingWindows")
  }
}
