package flink.mystream.sql.function

import org.apache.flink.table.api.scala._
import org.apache.flink.api.scala._
import org.apache.flink.types.Row
import flink.mystream.beans.SensorReading
import flink.mystream.utils.SensorReadingDataSource
import org.apache.flink.streaming.api.scala.{DataStream, StreamExecutionEnvironment}
import org.apache.flink.table.api.{DataTypes, EnvironmentSettings, Over, Table, TableEnvironment, Tumble}
import org.apache.flink.table.api.scala.StreamTableEnvironment
import org.apache.flink.table.expressions.TimeIntervalUnit
import org.apache.flink.table.types.DataType


case class Order(user: Long, product: String, amount: Int)

object T10_RANK {

  def main(args: Array[String]): Unit = {



  }
}
