package flink.mystream.sql.connector

import org.apache.flink.api.common.typeinfo.TypeInformation
import org.apache.flink.table.api.scala._
import org.apache.flink.api.scala._
import org.apache.flink.streaming.api.scala.{DataStream, StreamExecutionEnvironment}
import org.apache.flink.table.api.{DataTypes, EnvironmentSettings, Table}
import org.apache.flink.table.api.scala.StreamTableEnvironment
import org.apache.flink.table.descriptors.{Csv, Json, Kafka, Rowtime, Schema}
import org.apache.flink.table.sources.wmstrategies.WatermarkStrategy
import org.apache.flink.types.Row

object KafkaConnector {
  def main(args: Array[String]): Unit = {

    val environment: StreamExecutionEnvironment = StreamExecutionEnvironment.getExecutionEnvironment

    val environmentSettings: EnvironmentSettings = EnvironmentSettings.newInstance().useBlinkPlanner().inStreamingMode().build()
    val tableEnvironment: StreamTableEnvironment = StreamTableEnvironment.create(environment, environmentSettings)

    tableEnvironment
      .connect(new Kafka()
        .version("universal")
        .topic("first")
        .property("zookeeper.connect", "LocalOne:2181")
        .property("bootstrap.servers", "LocalOne:9092")
        .startFromEarliest())
      .withFormat(new Csv().deriveSchema())
      .withSchema(new Schema()
        .field("id", DataTypes.STRING)
        .field("timestamp", DataTypes.BIGINT).rowtime(new Rowtime().timestampsFromField("timestamp").watermarksPeriodicBounded(1000L))
        .field("temper", DataTypes.DOUBLE)
      )
      .createTemporaryTable("inputTableOne")

    //    val table: Table = tableEnvironment.sqlQuery("select * from inputTableOne")

    //    table.printSchema()
    //
    //    val resultSqlTable: Table = tableEnvironment.sqlQuery("select id, count(id) as cnt, avg(temper) as avgTemp, tumble_end(timestamp, interval '2' second) " + "from inputTableOne group by id, tumble(timestamp, interval '2' second)")
    //    resultSqlTable.toRetractStream[Row].print()

    tableEnvironment
      .connect(new Kafka()
        .version("universal")
        .topic("second")
        .property("zookeeper.connect", "LocalOne:2181")
        .property("bootstrap.servers", "LocalOne:9092")
        .startFromEarliest())
      .withFormat(new Csv().deriveSchema())
      .withSchema(new Schema()
        .field("id", DataTypes.STRING)
        .field("ts", DataTypes.BIGINT).rowtime(new Rowtime().timestampsFromField("ts").watermarksPeriodicBounded(1000L))
        .field("name", DataTypes.STRING())
      )
      .createTemporaryTable("inputTableTwo")


    //    此处做的是两个表的Regular Join，所以其需要保留所有的历史数据
    /**
     * 输入的过程如下
     * 给first的topic输入sensor_3,1599990790000,1.0。
     * 此时因为second的topic里面还没有其对应的维度表数据，所以其输出为
     * (true,sensor_3,1599990790000,1.0,null,null,null)
     *
     * second的topic输入
     * sensor_3,1599990790000,cc
     * 此时因为有了维度数据。所以此时会得到下面的输出。false表示以前的结果作废
     * (false,sensor_3,1599990790000,1.0,null,null,null)
     * (true,sensor_3,1599990790000,1.0,sensor_3,1599990790000,cc)
     *
     * 此时给first的topic输入sensor_3,1599990790000,2.0。
     * 则得到的输出如下。4> (true,sensor_3,1599990790000,2.0,sensor_3,1599990790000,cc)
     *
     *
     * 但是此时如果改变维度表的数据
     * 给second topic输入sensor_3,1599990790000,ee
     * 则此时得到的输出如下。可以看出其保留了双流里面的全部历史数据，任何一个流的数据输入都会驱动历史数据的变化。
     * 而且任何一个流的输入都会改变流的输出结果，所以其是双流驱动的
     * (true,sensor_3,1599990790000,2.0,sensor_3,1599990790000,ee)
     * (true,sensor_3,1599990790000,1.0,sensor_3,1599990790000,ee)
     */
    val table1: Table = tableEnvironment.sqlQuery("select * from inputTableOne t1 left join inputTableTwo t2 on t1.id=t2.id")
    table1.toRetractStream[Row].print()


    environment.execute()


  }
}
