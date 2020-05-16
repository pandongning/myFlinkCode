package flink.mystream.sql.sqlLanguage

import flink.mystream.beans.SensorReading
import flink.mystream.utils.SensorReadingDataSource
import org.apache.flink.streaming.api.scala.{DataStream, StreamExecutionEnvironment}
import org.apache.flink.table.api.{EnvironmentSettings, Over, Table, Tumble}
import org.apache.flink.table.api.scala.StreamTableEnvironment
import org.apache.flink.table.api.scala._
import org.apache.flink.api.scala._
import org.apache.flink.streaming.api.TimeCharacteristic
import org.apache.flink.types.Row


object T2_GroupBy {

  def main(args: Array[String]): Unit = {

    val environment: StreamExecutionEnvironment = StreamExecutionEnvironment.getExecutionEnvironment
    environment.setStreamTimeCharacteristic(TimeCharacteristic.EventTime)
    environment.setParallelism(3)

    val environmentSettings: EnvironmentSettings = EnvironmentSettings.newInstance().inStreamingMode().useBlinkPlanner().build()
    val tableEnvironment: StreamTableEnvironment = StreamTableEnvironment.create(environment,environmentSettings)

    val sensorReadingDataStreamOne: DataStream[SensorReading] = SensorReadingDataSource.getDataSource("LocalOne", 7778)

    // 此处得到结论，如果DataStream里面的event类型是scala的case class。
    // 则表里面字段的名字默认为class字段的名字。
    // 但是最好自己重新写一遍名字，因为我们需要指定eventTime字段。
    //  因为在DataStream里面已经指定了使用timestamp字段做为水印的时间戳，所以此处再次指定其为Table里面的eventTime

    //    此处的表对象是一个inlined table。即没有注册的表
    val tableOne: Table = sensorReadingDataStreamOne
      .toTable(tableEnvironment, 'id, 'timestamp.rowtime, 'temperature)

    val table: Table = tableEnvironment.sqlQuery(s"SELECT id,SUM(temperature) as sumTemperature FROM $tableOne group by id")

    /**
     * 输入数据
     * sensor_1,1585018995391,1.0
     * sensor_2,1585018995390,1.0
     * sensor_1,1585018995399,9.0
     * 输出的结果。false表示对原来的数据进行修改
     * (true,sensor_1,1.0)
     * (true,sensor_2,1.0)
     * (false,sensor_1,1.0)
     * (true,sensor_1,10.0)
     * 从上面的结果可以看出，如果要得到最新聚合的结果
     * 需要从上面过滤f0为false的记录
     * 并且对于每个id，取出最新的值，这个可以在输出的时候，覆盖原来的值做到
     */
    table.toRetractStream[Row].print()

    environment.execute()
  }
}
