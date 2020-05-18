//package flink.mystream.sql.tableapi
//
//import flink.mystream.beans.SensorReading
//import org.apache.flink.api.common.time.Time
//import org.apache.flink.streaming.api.TimeCharacteristic
//import org.apache.flink.streaming.api.functions.timestamps.BoundedOutOfOrdernessTimestampExtractor
//import org.apache.flink.streaming.api.scala.{DataStream, StreamExecutionEnvironment}
//import org.apache.flink.table.api.scala.StreamTableEnvironment
//import org.apache.flink.table.api.{EnvironmentSettings, Table}
//import org.apache.flink.types.Row
//import org.apache.flink.table.api.scala._
//import org.apache.flink.api.scala._
//
//object T1_DataStreamToTable {
//
//  def main(args: Array[String]): Unit = {
//
//    val environment: StreamExecutionEnvironment = StreamExecutionEnvironment.getExecutionEnvironment
//    environment.setStreamTimeCharacteristic(TimeCharacteristic.EventTime)
//
//    val environmentSettings: EnvironmentSettings = EnvironmentSettings.newInstance().inStreamingMode().useBlinkPlanner().build()
//    val tableEnvironment: StreamTableEnvironment = StreamTableEnvironment.create(environment, environmentSettings)
//
//    tableEnvironment.getConfig.setIdleStateRetentionTime(Time.minutes(20), Time.minutes(30))
//
//    val path: String = getClass.getClassLoader.getResource("sensor.txt").getPath
//    val sourceData: DataStream[String] = environment.readTextFile(path)
//
//    val sensorReadingDataStream: DataStream[SensorReading] = sourceData.map(line => {
//      val strings: Array[String] = line.split(",")
//      SensorReading(strings(0).trim, strings(1).trim.toLong, strings(2).trim.toDouble)
//    }).assignTimestampsAndWatermarks(new BoundedOutOfOrdernessTimestampExtractor[SensorReading](Time.milliseconds(1)) {
//      override def extractTimestamp(element: SensorReading): Long = element.timestamp
//    })
//
//
//    //    DataStream转换为Table
//    val tableOne: Table = sensorReadingDataStream.toTable(tableEnvironment)
//    tableOne.toRetractStream[SensorReading].print()
//    //    默认表的字段名字就是case class的字段名字
//    tableOne.select("timestamp").toRetractStream[Row].print()
//
//
//    //    Table转换为 DataStream。此处是将table里面的字段转换为Row对象。
//    tableOne.select('id, 'timestamp, 'temperature).toRetractStream[Row].print()
//    //    此处是将table里面的字段转换为SensorReading对象。table里面的字段和SensorReading里面的字段按照位置一一对应
//    tableOne.select('id, 'timestamp, 'temperature).toRetractStream[SensorReading].print()
//
//
//    // 此处得到结论，如果DataStream里面的event类型是scala的case class。
//    // 则表里面字段的名字默认为class字段的名字。
//    // 但是最好自己重新写一遍名字，因为我们需要指定eventTime字段。
//    //    timestamp.rowtime是将SensorReading对象里面的timestamp字段替换为eventTime字段。
//    val tableTwo: Table = sensorReadingDataStream
//      .toTable(tableEnvironment, 'id, 'timestamp.rowtime, 'temperature)
//    //    其结果和上面的tableOne结果一致。
//    tableTwo.toRetractStream[Row].print()
//    //    下面的代码是错误的，因为此时SensorReading里面timestamp的类型为long，但是在注册表的时候将其定义为时间类型
//    //    所以此时在tableTwo里面其类型已经变为TimeStamp类型，所以类型是不匹配的
//    //    tableTwo.toRetractStream[SensorReading].print()
//
//
//    //    新添加一个字段做为eventTime字段。其是从1970开始的
//    val tableThere: Table = sensorReadingDataStream
//      .toTable(tableEnvironment, 'id, 'timestamp, 'temperature, 'rowtime.rowtime)
//
//    tableThere.select('id, 'timestamp, 'temperature, 'rowtime).toRetractStream[Row].print()
//
//    environment.execute()
//
//  }
//
//
//}
