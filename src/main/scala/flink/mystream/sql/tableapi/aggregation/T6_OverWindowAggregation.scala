package flink.mystream.sql.tableapi.aggregation

import flink.mystream.beans.SensorReading
import org.apache.flink.api.common.typeinfo.{TypeInformation, Types}
import org.apache.flink.streaming.api.TimeCharacteristic
import org.apache.flink.streaming.api.functions.timestamps.BoundedOutOfOrdernessTimestampExtractor
import org.apache.flink.streaming.api.scala.{DataStream, StreamExecutionEnvironment}
import org.apache.flink.table.api.{EnvironmentSettings, Over, Table, Tumble}
import org.apache.flink.table.api.scala.StreamTableEnvironment
import org.apache.flink.table.api.scala._
import org.apache.flink.api.scala._
import org.apache.flink.streaming.api.windowing.time.Time
import org.apache.flink.table.functions.ScalarFunction
import org.apache.flink.types.Row

object T6_OverWindowAggregation {

  def main(args: Array[String]): Unit = {

    val environment: StreamExecutionEnvironment = StreamExecutionEnvironment.getExecutionEnvironment
    environment.setStreamTimeCharacteristic(TimeCharacteristic.EventTime)
    environment.setParallelism(1)

    val environmentSettings: EnvironmentSettings = EnvironmentSettings.newInstance().inStreamingMode().useBlinkPlanner().build()
    val tableEnvironment: StreamTableEnvironment = StreamTableEnvironment.create(environment, environmentSettings)


    val sourceData: DataStream[String] = environment.socketTextStream("LocalOne", 7777)

    val sensorReadingDataStream: DataStream[SensorReading] = sourceData.map(line => {
      val strings: Array[String] = line.split(",")
      SensorReading(strings(0).trim, strings(1).trim.toLong, strings(2).trim.toDouble)
    }).assignTimestampsAndWatermarks(new BoundedOutOfOrdernessTimestampExtractor[SensorReading](Time.milliseconds(1)) {
      override def extractTimestamp(element: SensorReading): Long = element.timestamp
    })


    // 此处得到结论，如果DataStream里面的event类型是scala的case class。
    // 则表里面字段的名字默认为class字段的名字。
    // 但是最好自己重新写一遍名字，因为我们需要指定eventTime字段。
    //  因为在DataStream里面已经指定了使用timestamp字段做为水印的时间戳，所以此处再次指定其为Table里面的eventTime
    val table: Table = sensorReadingDataStream
      .toTable(tableEnvironment, 'id, 'timestamp.rowtime, 'temperature)


    /**
     * 输入数据
     * sensor_1,1585018995390,1.0
     * sensor_1,1585018995392,1.0
     * sensor_1,1585018995394,1.0
     * sensor_1,1585018995395,3.0
     * sensor_1,1585018995396,4.0
     * sensor_1,1585018995397,5.0
     * 输出结果为
     * sensor_1,1.0
     * sensor_1,2.0
     * sensor_1,3.0
     * sensor_1,5.0
     * sensor_1,8.0
     * sensor_1,12.0
     *
     *
     * 所以over是一个开窗函数，下面的表达式表示当前行和其上面的2行对应的温度相加
     * Over partitionBy ("id") orderBy ("timestamp") preceding (2.rows) following (CURRENT_ROW) as 'w
     *
     * 下面的表达式表示当前行和其上面所有的行对应的温度相加
     * Over partitionBy ("id") orderBy ("timestamp") preceding (UNBOUNDED_ROW) following (CURRENT_ROW) as 'w
     */

    table
      .window(Over partitionBy ("id") orderBy ("timestamp") preceding (2.rows) following (CURRENT_ROW) as 'w)
      .select('id, 'temperature.sum over 'w)
      .toAppendStream[Row]
      .print()

    environment.execute()
  }
}

