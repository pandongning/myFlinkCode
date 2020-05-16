package flink.mystream.Accumulators

import org.apache.flink.api.common.JobExecutionResult
import org.apache.flink.api.common.accumulators.IntCounter
import org.apache.flink.api.common.functions.RichMapFunction
import org.apache.flink.streaming.api.scala.{DataStream, StreamExecutionEnvironment}
import org.apache.flink.streaming.api.scala._
import org.apache.flink.configuration.Configuration

object IntCounter {

  def main(args: Array[String]): Unit = {

    val environment: StreamExecutionEnvironment = StreamExecutionEnvironment.getExecutionEnvironment

    val wordDataStream: DataStream[String] = environment.fromElements("pdn", "hello", "word")

    val wordMapDataStream: DataStream[String] = wordDataStream.map(new RichMapFunction[String, String] {

      //      定义累加器
      private val numLines: IntCounter = new IntCounter

      //     注册累加器
      override def open(parameters: Configuration): Unit = {
        getRuntimeContext.addAccumulator("num-lines", this.numLines)
      }

      //  实现累加操做
      override def map(value: String): String = {
        this.numLines.add(1)
        value + "haha"
      }
    })

    wordMapDataStream.print()

    val jobExecutionResult: JobExecutionResult = environment.execute()

    //获取累加器的结果
    val intCounterResult: String = jobExecutionResult.getAccumulatorResult("num-lines").toString


    println("累加器的值为" + "\t" + intCounterResult)

  }
}
