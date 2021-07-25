package flink.mystream.sql.tableapi.distinctAggregation

import flink.mystream.beans.SensorReading
import flink.mystream.utils.SensorReadingDataSource
import org.apache.flink.streaming.api.scala.DataStream
import org.apache.flink.table.api.{Over, Table, Tumble}
import org.apache.flink.table.api.scala.StreamTableEnvironment
import org.apache.flink.table.api.scala._
import org.apache.flink.api.scala._
import org.apache.flink.types.Row

object T7_DistinctGroupByWindowOver {

  def main(args: Array[String]): Unit = {

    val sensorReadingDataStream: DataStream[SensorReading] = SensorReadingDataSource.getDataSource("LocalOne", 7777)
    val tableEnvironment: StreamTableEnvironment = SensorReadingDataSource.getTableEnvironment

    // 此处得到结论，如果DataStream里面的event类型是scala的case class。
    // 则表里面字段的名字默认为class字段的名字。
    // 但是最好自己重新写一遍名字，因为我们需要指定eventTime字段。
    //  因为在DataStream里面已经指定了使用timestamp字段做为水印的时间戳，所以此处再次指定其为Table里面的eventTime
    val table: Table = sensorReadingDataStream
      .toTable(tableEnvironment, 'id, 'timestamp.rowtime, 'temperature)

    table
      .groupBy('id)
      .select('id, 'temperature.sum.distinct)
      .toRetractStream[Row]
    //      .print()

    table
      .window(Tumble.over(3.millis).on('timestamp).as("w"))
      .groupBy('id, 'w)
      .select('id, 'temperature.sum.distinct)
      .toRetractStream[Row]
      .print()

    //    下面代码貌似有问题
    //    table
    //      .window(Over partitionBy ('id) orderBy ('timestamp) preceding (2.rows) following (CURRENT_ROW) as 'w)
    //      .select('id, 'temperature.avg.distinct over('w))
    //      .toRetractStream[Row]
    //      .print()

    //   所有字段都相同的，则只保留一条
    //    table.distinct().toRetractStream[Row].print()

    SensorReadingDataSource.environment.execute()
  }
}
