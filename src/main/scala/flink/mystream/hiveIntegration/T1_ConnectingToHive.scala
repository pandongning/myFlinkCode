package flink.mystream.hiveIntegration

import org.apache.flink.streaming.api.scala.StreamExecutionEnvironment
import org.apache.flink.table.api.EnvironmentSettings
import org.apache.flink.table.api.scala.StreamTableEnvironment
import org.apache.flink.table.catalog.hive.HiveCatalog

object T1_ConnectingToHive {
  def main(args: Array[String]): Unit = {
    val environment: StreamExecutionEnvironment = StreamExecutionEnvironment.getExecutionEnvironment
    environment.setParallelism(1)
    val settings = EnvironmentSettings.newInstance().useBlinkPlanner().inBatchMode().build()
    val tableEnvironment: StreamTableEnvironment = StreamTableEnvironment.create(environment, settings)


    val name = "myhive"
    val defaultDatabase = "mydatabase"
    val hiveConfDir = "src/main/resources" // a local path
    val version = "1.2.1"

    val hiveCatalog: HiveCatalog = new HiveCatalog(name, defaultDatabase, hiveConfDir, version)
    tableEnvironment.registerCatalog("MyHive", hiveCatalog)

    tableEnvironment.useCatalog("MyHive")
  }
}
