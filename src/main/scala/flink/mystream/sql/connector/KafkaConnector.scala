package flink.mystream.sql.connector

import org.apache.flink.table.api.scala._
import org.apache.flink.api.scala._
import org.apache.flink.streaming.api.scala.{DataStream, StreamExecutionEnvironment}
import org.apache.flink.table.api.{DataTypes, EnvironmentSettings, Table}
import org.apache.flink.table.api.scala.StreamTableEnvironment
import org.apache.flink.table.descriptors.{Json, Kafka, Rowtime, Schema}
import org.apache.flink.types.Row

object KafkaConnector {
  def main(args: Array[String]): Unit = {

    val environment: StreamExecutionEnvironment = StreamExecutionEnvironment.getExecutionEnvironment

    val environmentSettings: EnvironmentSettings = EnvironmentSettings.newInstance().useBlinkPlanner().inStreamingMode().build()
    val tableEnvironment: StreamTableEnvironment = StreamTableEnvironment.create(environment, environmentSettings)

    tableEnvironment
      .connect(new Kafka()
        .version("0.11")
        .topic("first")
        .property("zookeeper.connect", "LocalOne:2181,LocalTwo:2181,LocalThere:2181")
        .property("bootstrap.servers", "LocalOne:9092,LocalTwo:9092,LocalThere:9092")
        .property("group.id", "pdn2")
        .startFromLatest()
        .sinkPartitionerRoundRobin()
      )
      .inRetractMode()
      .withFormat(
        new Json()
          .failOnMissingField(false)
          .jsonSchema(
            "{" +
              "  type: 'object'," +
              "  properties: {" +
              "    id: {" +
              "      type: 'STRING'" +
              "    }," +
              "    myTimestamp: {" +
              "      type: 'string'," +
              "    }" +
              "    temperature: {" +
              "      type: 'DECIMAL'," +
              "    }" +
              "  }" +
              "}"
          )
      )
      .withSchema(new Schema()
        .field("id", DataTypes.INT())
        .field("myTimestamp", DataTypes.BIGINT())
        .field("temperature", DataTypes.DOUBLE())
        .rowtime(new Rowtime()
          .timestampsFromField("myTimestamp")
          .watermarksPeriodicBounded(1L)
        )
      )
      .createTemporaryTable("tabel")

    val table: Table = tableEnvironment.sqlQuery("select * from tabel")
    table.toRetractStream[Row].print()

    environment.execute()



  }
}
