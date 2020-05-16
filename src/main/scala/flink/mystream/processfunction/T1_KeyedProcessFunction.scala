package flink.mystream.processfunction

import flink.mystream.beans.SensorReading
import org.apache.flink.api.common.state.{ValueState, ValueStateDescriptor}
import org.apache.flink.streaming.api.TimeCharacteristic
import org.apache.flink.streaming.api.functions.KeyedProcessFunction
import org.apache.flink.streaming.api.functions.timestamps.BoundedOutOfOrdernessTimestampExtractor
import org.apache.flink.streaming.api.scala.{DataStream, StreamExecutionEnvironment}
import org.apache.flink.streaming.api.scala._
import org.apache.flink.streaming.api.windowing.time.Time
import org.apache.flink.util.Collector

object T1_KeyedProcessFunction {

  def main(args: Array[String]): Unit = {
    val environment: StreamExecutionEnvironment = StreamExecutionEnvironment.getExecutionEnvironment

    environment.setStreamTimeCharacteristic(TimeCharacteristic.EventTime)

    val line: DataStream[String] = environment.socketTextStream("192.168.48.1", 8889)

    val sensorReadingDataStream: DataStream[SensorReading] = line.map(
      data => {
        val dataArray: Array[String] = data.split(",")
        SensorReading(dataArray(0).trim, dataArray(1).trim.toLong, dataArray(2).trim.toDouble)
      }
    ).assignTimestampsAndWatermarks(new BoundedOutOfOrdernessTimestampExtractor[SensorReading](Time.seconds(5)) {
      override def extractTimestamp(sensorReading: SensorReading): Long = {
        sensorReading.timestamp
      }
    })

    val sensorReadingKeyedStream: KeyedStream[SensorReading, String] = sensorReadingDataStream.keyBy(_.id)

    val value: DataStream[String] = sensorReadingKeyedStream.process(new TempIncreAlert)

    value.print()

    environment.execute()

  }
}


class TempIncreAlert() extends KeyedProcessFunction[String, SensorReading, String] {

  private lazy val lastTemp: ValueState[Double] = getRuntimeContext.getState(new ValueStateDescriptor[Double]("lastTemp", classOf[Double]))
  //  获得定时器的时间
  private lazy val currentTimer: ValueState[Long] = getRuntimeContext.getState(new ValueStateDescriptor[Long]("currentTimer", classOf[Long]))

  override def processElement(value: SensorReading, ctx: KeyedProcessFunction[String, SensorReading, String]#Context, out: Collector[String]): Unit = {

    val preTemp: Double = lastTemp.value()
    lastTemp.update(value.temperature)

    val curTimerTs: Long = currentTimer.value()

    val currentTemperature: Double = value.temperature

    if (preTemp < currentTemperature || preTemp == 0) {
      ctx.timerService().deleteEventTimeTimer(curTimerTs)
      currentTimer.clear()
    } else if (preTemp > currentTemperature && curTimerTs == 0) {
      val timerTs: Long = ctx.timerService().currentWatermark() + 5000L
      ctx.timerService().registerEventTimeTimer(timerTs)
      currentTimer.update(timerTs)
    }


  }

  override def onTimer(timestamp: Long, ctx: KeyedProcessFunction[String, SensorReading, String]#OnTimerContext, out: Collector[String]): Unit = {
    out.collect(ctx.getCurrentKey + "温度连续上升")
    //    报警之后则应该清空状态信息
    currentTimer.clear()
  }
}