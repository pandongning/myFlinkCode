package flink.mystream.sql.tableapi

import flink.mystream.beans.SensorReading
import org.apache.flink.api.common.time.Time
import org.apache.flink.streaming.api.TimeCharacteristic
import org.apache.flink.streaming.api.functions.timestamps.BoundedOutOfOrdernessTimestampExtractor
import org.apache.flink.streaming.api.scala.{DataStream, StreamExecutionEnvironment}
import org.apache.flink.table.api.scala.StreamTableEnvironment
import org.apache.flink.table.api.{EnvironmentSettings, Table}
import org.apache.flink.types.Row
import org.apache.flink.table.api.scala._
import org.apache.flink.api.scala._

object T1_DataStreamToTable {

  def main(args: Array[String]): Unit = {

    val environment: StreamExecutionEnvironment = StreamExecutionEnvironment.getExecutionEnvironment
    environment.setStreamTimeCharacteristic(TimeCharacteristic.EventTime)

    val environmentSettings: EnvironmentSettings = EnvironmentSettings.newInstance().inStreamingMode().useBlinkPlanner().build()
    val tableEnvironment: StreamTableEnvironment = StreamTableEnvironment.create(environment, environmentSettings)

    tableEnvironment.getConfig.setIdleStateRetentionTime(Time.minutes(20), Time.minutes(30))

    val path: String = getClass.getClassLoader.getResource("sensor.txt").getPath
    val sourceData: DataStream[String] = environment.readTextFile(path)

    val sensorReadingDataStream: DataStream[SensorReading] = sourceData.map((line: String) => {
      val strings: Array[String] = line.split(",")
      SensorReading(strings(0).trim, strings(1).trim.toLong, strings(2).trim.toDouble)
    }).assignTimestampsAndWatermarks(new BoundedOutOfOrdernessTimestampExtractor[SensorReading](org.apache.flink.streaming.api.windowing.time.Time.minutes(1)) {
      override def extractTimestamp(element: SensorReading): Long = element.timestamp
    })


    //    DataStream转换为Table
    val tableOne: Table = sensorReadingDataStream.toTable(tableEnvironment)

    //    tableOne.printSchema()

    /**
     * root
     * |-- id: STRING
     * |-- timestamp: BIGINT
     * |-- temperature: DOUBLE
     */

    //    tableOne.toRetractStream[SensorReading].print()

    /**
     * 5> (true,SensorReading(sensor_1,1547718206,2.0))
     * 3> (true,SensorReading(sensor_10,1547718205,3.0))
     * 6> (true,SensorReading(sensor_1,1547718207,3.0))
     * 2> (true,SensorReading(sensor_7,1547718202,2.0))
     * 8> (true,SensorReading(sensor_1,1547718199,1.0))
     * 1> (true,SensorReading(sensor_6,1547718201,1.0))
     */

    //    默认表的字段名字就是case class的字段名字
    //    tableOne.select("timestamp").toRetractStream[Row].print()

    /**
     * 6> (true,1547718202)
     * 2> (true,1547718207)
     * 4> (true,1547718199)
     * 1> (true,1547718206)
     * 7> (true,1547718205)
     * 5> (true,1547718201)
     */
    //    Table转换为 DataStream。此处是将table里面的字段转换为Row对象。
    val value: DataStream[(Boolean, Row)] = tableOne.select('id, 'timestamp, 'temperature).toRetractStream[Row]
    val value1: DataStream[Row] = tableOne.select('id, 'timestamp, 'temperature).toAppendStream[Row]
    value1.map((row: Row) => (row.getField(0), row.getField(1)))

    /**
     * (sensor_1,1547718199)
     * (sensor_10,1547718205)
     * (sensor_6,1547718201)
     * (sensor_7,1547718202)
     */


    //    此处是将table里面的字段转换为SensorReading对象。table里面的字段和SensorReading里面的字段按照位置一一对应
    //    只要table里面的数据类型和case class里面的数据类型保持一致，则就可以进行转换
    //    tableOne.select('id, 'timestamp, 'temperature).toRetractStream[SensorReading].print()
    /**
     * (true,SensorReading(sensor_10,1547718205,3.0))
     * (true,SensorReading(sensor_1,1547718206,2.0))
     * (true,SensorReading(sensor_1,1547718207,3.0))
     * (true,SensorReading(sensor_1,1547718199,1.0))
     * (true,SensorReading(sensor_6,1547718201,1.0))
     * (true,SensorReading(sensor_7,1547718202,2.0))
     */


    // 此处得到结论，如果DataStream里面的event类型是scala的case class。
    // 则表里面字段的名字默认为class字段的名字。
    // 但是最好自己重新写一遍名字，因为我们需要指定eventTime字段。
    //    timestamp.rowtime是将SensorReading对象里面的timestamp字段替换为eventTime字段。
    val tableTwo: Table = sensorReadingDataStream
      .toTable(tableEnvironment, 'id, 'timestamp.rowtime, 'temperature)
    tableTwo.printSchema()

    /**
     * 可以看出，以前时间字段类型是Long，现在变为TIMESTAMP(3)
     * root
     * |-- id: STRING
     * |-- timestamp: TIMESTAMP(3) *ROWTIME*
     * |-- temperature: DOUBLE
     */


    //    其结果和上面的tableOne结果一致。
//    tableTwo.toRetractStream[Row].print()
    /**
     * 可以看出其将时间戳变为了其对应的日期进行输出的。可以看出输出的时间里面有个T，
     * 日期和时间的组合表示法编辑
     * 合并表示时，要在时间前面加一大写字母T，如要表示北京时间2004年5月3日下午5点30分8秒，可以写成2004-05-03T17:30:08+08:00或20040503T173008+08。
     * (true,sensor_1,1970-01-18T21:55:18.206,2.0)
     * (true,sensor_1,1970-01-18T21:55:18.199,1.0)
     * (true,sensor_7,1970-01-18T21:55:18.202,2.0)
     * (true,sensor_10,1970-01-18T21:55:18.205,3.0)
     * (true,sensor_1,1970-01-18T21:55:18.207,3.0)
     * (true,sensor_6,1970-01-18T21:55:18.201,1.0)
     */

    //    下面的代码是错误的，因为此时SensorReading里面timestamp的类型为long，但是在注册表的时候将其定义为时间类型
    //    所以此时在tableTwo里面其类型已经变为TimeStamp(3)类型，所以类型是不匹配的
    //    tableTwo.toRetractStream[SensorReading].print()


    //    新添加一个字段做为eventTime字段。其是从1970开始的
    val tableThere: Table = sensorReadingDataStream
      .toTable(tableEnvironment, 'id, 'timestamp, 'temperature, 'rowtime.rowtime)


    tableThere.select('id, 'timestamp, 'temperature, 'rowtime).toRetractStream[Row].print()

    environment.execute()

  }


}
