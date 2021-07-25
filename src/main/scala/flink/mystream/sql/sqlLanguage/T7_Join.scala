package flink.mystream.sql.sqlLanguage

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


object T7_Join {
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

    tableEnvironment.createTemporaryView("tableOne", tableOne)
    tableEnvironment.createTemporaryView("tableTwo", tableTwo)


    /**
     * 左右2条流都可以触发计算
     * 左流输入sensor_1,1547718199,1.0
     * 输出
     * 3> (true,sensor_1,1547718199,1.0,null,null,null)
     *
     *
     *
     * 右流输入
     * sensor_1,1547718199,1.1
     * 输出
     * 3> (false,sensor_1,1547718199,1.0,null,null,null)
     * 3> (true,sensor_1,1547718199,1.0,sensor_1,1547718199,1.1)
     *
     *
     * 右流继续输入sensor_1,1547718199,1.2
     * 则有输出
     * 3> (true,sensor_1,1547718199,1.0,sensor_1,1547718199,1.2)
     *
     * 可以看出左流也触发了执行
     *
     */
    tableEnvironment.sqlQuery(
      """
        |select t1.*,t2.*
        | from tableOne t1 left join tableTwo  t2 on t1.id = t2.idTwo
        |""".stripMargin).toRetractStream[Row].print()

    environment.execute()


  }

}
