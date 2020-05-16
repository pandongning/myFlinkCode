package flink.mystream.join

import org.apache.flink.streaming.api.TimeCharacteristic
import org.apache.flink.streaming.api.functions.co.ProcessJoinFunction
import org.apache.flink.streaming.api.functions.timestamps.AscendingTimestampExtractor
import org.apache.flink.streaming.api.scala.StreamExecutionEnvironment
import org.apache.flink.streaming.api.scala._
import org.apache.flink.streaming.api.windowing.time.Time
import org.apache.flink.util.Collector

object IntervalJoin {

  def main(args: Array[String]): Unit = {
    val environment: StreamExecutionEnvironment = StreamExecutionEnvironment.getExecutionEnvironment

    environment.setStreamTimeCharacteristic(TimeCharacteristic.EventTime)


    val leftKeyedStream: KeyedStream[Transcript, String] = environment.fromCollection(transcripts).assignTimestampsAndWatermarks(new AscendingTimestampExtractor[Transcript] {
      override def extractAscendingTimestamp(element: Transcript): Long = element.time
    }).keyBy(_.id)

    val rightKeyedStream: KeyedStream[Student, String] = environment.fromCollection(students).assignTimestampsAndWatermarks(new AscendingTimestampExtractor[Student] {
      override def extractAscendingTimestamp(element: Student): Long = element.time
    }).keyBy(_.id)


    val joinDataStream: DataStream[(String, String, String, String, Integer)] = leftKeyedStream.intervalJoin(rightKeyedStream)
      .between(Time.milliseconds(-2), Time.milliseconds(2))
      .lowerBoundExclusive()
      .upperBoundExclusive()
      .process(new ProcessJoinFunction[Transcript, Student, (String, String, String, String, Integer)] {
        override def processElement(transcript: Transcript, student: Student, ctx: ProcessJoinFunction[Transcript, Student, (String, String, String, String, Integer)]#Context, out: Collector[(String, String, String, String, Integer)]): Unit = {
          out.collect((transcript.id, transcript.name, student.className, transcript.subject, transcript.score))
        }
      })

    joinDataStream.print()

    environment.execute()

  }

  private val transcripts: Array[Transcript] = Array(
    Transcript("1", "张三", "语文", 100, System.currentTimeMillis()),
    Transcript("2", "李四", "语文", 78, System.currentTimeMillis()),
    Transcript("3", "王五", "语文", 99, System.currentTimeMillis()),
    Transcript("4", "赵六", "语文", 81, System.currentTimeMillis()),
    Transcript("5", "钱七", "语文", 59, System.currentTimeMillis()),
    Transcript("6", "马二", "语文", 97, System.currentTimeMillis()))

  private val students: Array[Student] = Array(
    Student("1", "张三", "class1", System.currentTimeMillis),
    Student("2", "李四", "class1", System.currentTimeMillis),
    Student("3", "王五", "class1", System.currentTimeMillis),
    Student("4", "赵六", "class2", System.currentTimeMillis),
    Student("5", "钱七", "class2", System.currentTimeMillis),
    Student("6", "马二", "class2", System.currentTimeMillis))
}
