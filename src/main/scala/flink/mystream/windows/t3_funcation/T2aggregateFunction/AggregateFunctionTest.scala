package flink.mystream.windows.t3_funcation.T2aggregateFunction

import org.apache.flink.streaming.api.scala.{DataStream, StreamExecutionEnvironment}
import org.apache.flink.streaming.api.windowing.time.Time

object AggregateFunctionTest {
  def main(args: Array[String]): Unit = {
    val environment: StreamExecutionEnvironment = StreamExecutionEnvironment.getExecutionEnvironment

    val text: DataStream[String] = environment.socketTextStream("LocalOne", 9888)

    import org.apache.flink.api.scala._
    text.flatMap((_: String).split(","))
      .map((_, 1L))
      .keyBy(0)
      .timeWindow(Time.seconds(5))
      .aggregate(new AverageAggregate)
      .print()

    environment.execute()
  }
}
