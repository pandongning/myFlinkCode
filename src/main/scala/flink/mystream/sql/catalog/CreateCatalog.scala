package flink.mystream.sql.catalog

import org.apache.flink.streaming.api.scala.StreamExecutionEnvironment

object CreateCatalog {
  def main(args: Array[String]): Unit = {

    val environment: StreamExecutionEnvironment = StreamExecutionEnvironment.getExecutionEnvironment
    environment.setParallelism(1)

//    val settings: EnvironmentSettings = EnvironmentSettings.newInstance().inStreamingMode().useBlinkPlanner().build()
//    val tableEnvironment: StreamTableEnvironment = StreamTableEnvironment.create(environment, settings)
//
//    val name = "myhive"
//    val version = "1.2.1"
//    tableEnvironment.loadModule(name,new HiveModule(version))
  }
}
