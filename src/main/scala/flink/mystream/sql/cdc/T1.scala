package flink.mystream.sql.cdc

import org.apache.flink.api.common.restartstrategy.RestartStrategies
import org.apache.flink.api.scala.createTypeInformation
import org.apache.flink.streaming.api.CheckpointingMode
import org.apache.flink.streaming.api.environment.CheckpointConfig.ExternalizedCheckpointCleanup
import org.apache.flink.streaming.api.scala.StreamExecutionEnvironment
import org.apache.flink.table.api.{EnvironmentSettings, Table}
import org.apache.flink.table.api.scala.StreamTableEnvironment

object T1 {
  def main(args: Array[String]): Unit = {
    val environment: StreamExecutionEnvironment = StreamExecutionEnvironment.getExecutionEnvironment

    environment.enableCheckpointing(5000L)
    environment.getCheckpointConfig.setCheckpointingMode(CheckpointingMode.EXACTLY_ONCE)
    environment.getCheckpointConfig.setCheckpointTimeout(10000L)
    environment.getCheckpointConfig.setFailOnCheckpointingErrors(false)
    environment.getCheckpointConfig.setMaxConcurrentCheckpoints(1)
    environment.getCheckpointConfig.enableExternalizedCheckpoints(ExternalizedCheckpointCleanup.DELETE_ON_CANCELLATION)
    //    失败之后自动重启3次，每次的间隔设置为120s。超过3次则直接判定任务失败。
    environment.setRestartStrategy(RestartStrategies.fixedDelayRestart(3, org.apache.flink.api.common.time.Time.seconds(120)))


    val environmentSettings: EnvironmentSettings = EnvironmentSettings.newInstance().useBlinkPlanner().inStreamingMode().build()
    val tableEnvironment: StreamTableEnvironment = StreamTableEnvironment.create(environment, environmentSettings)


    tableEnvironment.sqlUpdate("CREATE TABLE products (id INT,name STRING,description STRING) WITH ('connector' = 'mysql-cdc','hostname' = 'LocalOne','port' = '3306','username' = 'root','password' = 'root','database-name' = 'mydb','table-name' = 'products')")

    val table: Table = tableEnvironment.sqlQuery("select * from products")



//    import com.alibaba.ververica.cdc.connectors.mysql.MySQLSource
//    import com.alibaba.ververica.cdc.debezium.StringDebeziumDeserializationSchema
//    import org.apache.flink.streaming.api.functions.source.SourceFunction
//    val sourceFunction: SourceFunction[String] = MySQLSource.builder[String].hostname("LocalOne").port(3306).databaseList("mydb")
//      .username("root").password("root").deserializer(new StringDebeziumDeserializationSchema).build
//
//
//
//    environment.addSource(sourceFunction).print.setParallelism(1) // use parallelism 1 for sink to keep message ordering


    environment.execute()

  }

}
