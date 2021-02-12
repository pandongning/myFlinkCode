import java.sql.Timestamp
import java.util

import org.apache.flink.streaming.api.scala.{DataStream, SplitStream, StreamExecutionEnvironment}
import org.apache.flink.api.scala._
import org.apache.flink.streaming.api.TimeCharacteristic
import org.apache.flink.streaming.api.datastream.DataStreamUtils
import org.apache.flink.streaming.api.functions.timestamps.BoundedOutOfOrdernessTimestampExtractor
import org.apache.flink.streaming.api.windowing.time.Time

object Split {

  def main(args: Array[String]): Unit = {

    //1612969006000
    println((1599990791000L) - (28800000 + (1599990791000L) + 7000) % 7000)

    //  1599990789000
    println((1599990791000L) - (1599990791000L + 3000) % 3000)

    //1599990792000
    println(1599990792000L - (1599990792000L + 3000) % 3000)

    println(System.currentTimeMillis())

  }
}
