package flink.mystream.windows.t3_funcation

import flink.mystream.utils.FlinkKafkaUtil
import org.apache.flink.streaming.api.TimeCharacteristic
import org.apache.flink.streaming.api.functions.timestamps.BoundedOutOfOrdernessTimestampExtractor
import org.apache.flink.streaming.api.scala.{DataStream, StreamExecutionEnvironment}
import org.apache.flink.streaming.api.scala._
import org.apache.flink.streaming.api.scala.function.{ProcessAllWindowFunction, ProcessWindowFunction}
import org.apache.flink.streaming.api.windowing.time.Time
import org.apache.flink.streaming.api.windowing.windows.TimeWindow
import org.apache.flink.util.Collector

case class UserBehavior(userId: Long, itemId: Long, categoryId: Int, behavior: String, timestamp: Long)

/**
 * 对于keyBy和非keyBy的process函数，深刻理解、
 */
object HotItems {

  def main(args: Array[String]): Unit = {

    val environment: StreamExecutionEnvironment = FlinkKafkaUtil.getEnvironment
    environment.setStreamTimeCharacteristic(TimeCharacteristic.EventTime)

    val line: DataStream[String] = FlinkKafkaUtil.createKafkaSoureStream(args)

    val lineWithWaterMaker: DataStream[String] = line.assignTimestampsAndWatermarks(new BoundedOutOfOrdernessTimestampExtractor[String](Time.milliseconds(1)) {
      override def extractTimestamp(element: String): Long = {
        element.split(",")(4).toLong
      }
    })


    val dataStream: DataStream[UserBehavior] = lineWithWaterMaker.map(
      line => {
        val strings: Array[String] = line.split(",")
        UserBehavior(strings(0).toLong, strings(1).toLong, strings(2).toInt, strings(3).toString, strings(4).toLong)
      }
    )

    //此处的结论为，对于没有按照key进行分组的流，直接keyBy，则process函数里面的elements表示一个组里面的全部event
    /**
     * dataStream
     * .timeWindowAll(Time.milliseconds(3))
     * .process(new ProcessAllWindowFunction[UserBehavior, String, TimeWindow]() {
     * override def process(context: Context, elements: Iterable[UserBehavior], out: Collector[String]): Unit = {
     *           out.collect(elements.toList.mkString("\t"))
     * }
     * })
     * .print()
     */


    /**
     * 此处的结论为，如果对数据流进行了keyBy。则对于每个窗口。如果此窗口里面有多个key。
     * 当窗口达到kafka的触发条件的时候，每个key都会触发一次process函数。
     * 其里面的参数elements表示此窗口里面对应key的全部event
     */

    dataStream
      .keyBy(_.itemId)
      .timeWindow(Time.milliseconds(3))
      .process(new MyProcessWindowFunctionTwo)
      .print()


    environment.execute("HotItems")
  }
}

class MyProcessWindowFunctionTwo extends ProcessWindowFunction[UserBehavior, String, Long, TimeWindow] {
  var i = 0

  override def process(key: Long, context: Context, elements: Iterable[UserBehavior], out: Collector[String]): Unit = {
    i += 1
    out.collect(i + "\t" + key + "\t" + elements.toList)
  }
}
