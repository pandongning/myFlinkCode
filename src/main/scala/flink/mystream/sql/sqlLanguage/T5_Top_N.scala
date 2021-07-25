package flink.mystream.sql.sqlLanguage

import flink.mystream.beans.SensorReading
import flink.mystream.utils.SensorReadingDataSource
import org.apache.flink.streaming.api.scala.{DataStream, StreamExecutionEnvironment}
import org.apache.flink.table.api.{Over, Table, Tumble}
import org.apache.flink.table.api.scala.StreamTableEnvironment
import org.apache.flink.table.api.scala._
import org.apache.flink.api.scala._
import org.apache.flink.types.Row

object T5_Top_N {

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

    tableEnvironment.createTemporaryView("tableOne", tableOne)

    /**
     * 对于同一个key，随时的一条输入，都会影响以前的结果
     * 所以此时最好设置状态的存活时间。
     * 因为此处得到的是从历史到现在的top3
     *
     * 输入sensor_6, 1547718201, 1.1
     * 输出2  (true,sensor_6,1970-01-18T21:55:18.201,1.2,1)
     * 再次输入sensor_6, 1547718201, 1.2
     * 输出。即提前排名为1的，此时排名为2呢
     * (false,sensor_6,1970-01-18T21:55:18.201,1.1,1)
     * (true,sensor_6,1970-01-18T21:55:18.201,1.1,2)
     */

    tableEnvironment.sqlQuery(
      """
        |select * from (
        |select *, row_number() over (partition by id order by temperature desc) as rk from tableOne
        |) t1 where rk<=3
        |
        |""".stripMargin).toRetractStream[Row].print()


    environment.execute()
  }
}
