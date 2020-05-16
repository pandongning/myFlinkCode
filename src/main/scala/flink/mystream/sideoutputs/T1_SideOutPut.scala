package flink.mystream.sideoutputs

import org.apache.flink.streaming.api.functions.ProcessFunction
import org.apache.flink.streaming.api.scala.StreamExecutionEnvironment
import org.apache.flink.streaming.api.scala._
import org.apache.flink.util.Collector

object T1_SideOutPut {

  def main(args: Array[String]): Unit = {

    val environment: StreamExecutionEnvironment = StreamExecutionEnvironment.getExecutionEnvironment


    val source: DataStream[Int] = environment.fromElements(1, 2, 3, 4)

    val sideTag: OutputTag[Int] = new OutputTag[Int]("odd")

    val value1: DataStream[Int] = source.process(new MyProcessFunction(sideTag))

    val value2: DataStream[Int] = value1.getSideOutput(sideTag)

    value1.print("value1")
    value2.print("value2")

    //    将下面的json串复制到https://flink.apache.org/visualizer/，则可以查看执行计划
    val plan: String = environment.getExecutionPlan
    println(plan)


    environment.execute()
  }
}

private class MyProcessFunction(sideTag: OutputTag[Int]) extends ProcessFunction[Int, Int] {
  //所以此处可以看出，侧输出流不一定用于得到迟到的数据，也可以用于将原始数据分为几个流输出
  override def processElement(value: Int, ctx: ProcessFunction[Int, Int]#Context, out: Collector[Int]): Unit = {
    if (value / 2 == 0) {
      out.collect(value)
    } else {
      ctx.output(sideTag, value + 100)
    }
  }
}
