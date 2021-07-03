package flink.mystream.sql.sqlLanguage

import flink.mystream.beans.SensorReading
import flink.mystream.utils.SensorReadingDataSource
import org.apache.flink.streaming.api.scala.{DataStream, StreamExecutionEnvironment}
import org.apache.flink.table.api.{Over, Table, Tumble}
import org.apache.flink.table.api.scala.StreamTableEnvironment
import org.apache.flink.table.api.scala._
import org.apache.flink.api.scala._
import org.apache.flink.types.Row


object T3_GroupByTUMBLEWindowAggregation {
  def main(args: Array[String]): Unit = {

    val environment: StreamExecutionEnvironment = SensorReadingDataSource.environment
    val tableEnvironment: StreamTableEnvironment = SensorReadingDataSource.getTableEnvironment

    val sensorReadingDataStreamOne: DataStream[SensorReading] = SensorReadingDataSource.getDataSource("LocalOne", 7778)

    // 此处得到结论，如果DataStream里面的event类型是scala的case class。
    // 则表里面字段的名字默认为class字段的名字。
    // 但是最好自己重新写一遍名字，因为我们需要指定eventTime字段。
    //  因为在DataStream里面已经指定了使用timestamp字段做为水印的时间戳，所以此处再次指定其为Table里面的eventTime

    //    此处的表对象是一个inlined table。即没有注册的表
    val tableOne: Table = sensorReadingDataStreamOne
      .toTable(tableEnvironment, 'id, 'timestamp.rowtime, 'temperature)

    /**
     * 输入
     * >sensor_1, 1599990790000,1
     * >sensor_1, 1599990790000,1.1
     * >sensor_1, 1599990790000,1.2
     * >sensor_2, 1599990792000,2
     * >sensor_2, 1599990792000,2.1
     * >sensor_2, 1599990793000,2.2
     * 得到输出
     * 8> (true,sensor_1,3.3,+52671-09-09T22:06:40,+52671-09-09T22:06:42)
     * 但是继续输入迟到的数据
     * sensor_1, 1599990790000,1.4
     * 其并不会再次触发窗口的执行。所以可以对于窗口的聚合，其不会处理迟到的数据
     */
    val table: Table = tableEnvironment
      .sqlQuery(
        s"""
           |select id,sum(temperature),
           |TUMBLE_START(`timestamp`, INTERVAL '2' SECOND) as wStart,
           |TUMBLE_END(`timestamp`, INTERVAL '2' SECOND) as wEnd
           |from $tableOne
           |group by TUMBLE(`timestamp`, INTERVAL '2' SECOND),id
           |""".stripMargin)

    table.toRetractStream[Row].print()

    environment.execute()
  }
}
