package flink.mystream.sql.tableapi.joins

import flink.mystream.beans.SensorReading
import flink.mystream.utils.SensorReadingDataSource
import flink.mystream.utils.SensorReadingDataSource.environment
import org.apache.flink.streaming.api.scala.{DataStream, StreamExecutionEnvironment}
import org.apache.flink.table.api.{EnvironmentSettings, Over, Table, Tumble}
import org.apache.flink.table.api.scala.StreamTableEnvironment
import org.apache.flink.table.api.scala._
import org.apache.flink.api.scala._
import org.apache.flink.streaming.api.functions.timestamps.BoundedOutOfOrdernessTimestampExtractor
import org.apache.flink.streaming.api.windowing.time.Time
import org.apache.flink.types.Row

object T8_InnerJoin {
  def main(args: Array[String]): Unit = {

    val environment: StreamExecutionEnvironment = SensorReadingDataSource.environment
    val tableEnvironment: StreamTableEnvironment = SensorReadingDataSource.getTableEnvironment


    val sensorReadingDataStream: DataStream[SensorReading] = SensorReadingDataSource.getDataSource("LocalOne", 7778)

    // 此处得到结论，如果DataStream里面的event类型是scala的case class。
    // 则表里面字段的名字默认为class字段的名字。
    // 但是最好自己重新写一遍名字，因为我们需要指定eventTime字段。
    //  因为在DataStream里面已经指定了使用timestamp字段做为水印的时间戳，所以此处再次指定其为Table里面的eventTime
    val tableOne: Table = sensorReadingDataStream
      .toTable(tableEnvironment, 'id, 'timestamp, 'temperature)

    val soureTow: DataStream[String] = environment.socketTextStream("LocalOne", 7777)
    val soureTowWithWaterMaker: DataStream[SensorReading] = soureTow.map(line => {
      val strings: Array[String] = line.split(",")
      SensorReading(strings(0).trim, strings(1).trim.toLong, strings(2).trim.toDouble)
    }).assignTimestampsAndWatermarks(new BoundedOutOfOrdernessTimestampExtractor[SensorReading](Time.milliseconds(1)) {
      override def extractTimestamp(element: SensorReading): Long = element.timestamp
    })

    val tableTwo: Table = soureTowWithWaterMaker.toTable(tableEnvironment, 'idTwo, 'timestampTwo, 'temperatureTwo)

    //    tableOne
    //      .join(tableTwo)
    //      .where('id === 'idTwo)
    //      .toRetractStream[Row]
    //      .print()

    /**
     * 左流和右流的输入都可以触发
     * join动作
     * 流tableOne输入sensor_1,1547718199,1.0
     * 输出
     * (true,sensor_1,1547718199,1.0,null,null,null)
     *
     * tableTwo输入sensor_1,1547718199,1.1
     * 输出。所以此时则说明了左流的输入也触发了join计算
     * (false,sensor_1,1547718199,1.0,null,null,null)
     * (true,sensor_1,1547718199,1.0,sensor_1,1547718199,1.1)
     *
     * 如果流tableTwo输入sensor_1,1547718199,1.1
     * 则此时如果tableOne输入sensor_1,1547718199,1.3
     * 输出。因为此时右变的流里面有2条对应的数据
     * 3> (true,sensor_1,1547718199,1.3,sensor_1,1547718199,1.1)
     * 3> (true,sensor_1,1547718199,1.3,sensor_1,1547718199,1.0)
     */
    tableOne.leftOuterJoin(tableTwo, 'id === 'idTwo)
      .toRetractStream[Row]
      .print()

    //    tableOne
    //      .fullOuterJoin(tableTwo, 'id === 'idTwo)
    //      .toRetractStream[Row]
    //      .print()

    environment.execute()
  }
}
