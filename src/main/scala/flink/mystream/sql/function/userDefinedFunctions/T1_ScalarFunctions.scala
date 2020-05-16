package flink.mystream.sql.function.userDefinedFunctions

import org.apache.flink.table.api.scala._
import org.apache.flink.api.scala._
import org.apache.flink.types.Row
import flink.mystream.beans.SensorReading
import flink.mystream.utils.SensorReadingDataSource
import org.apache.flink.api.common.typeinfo.{TypeInformation, Types}
import org.apache.flink.streaming.api.scala.{DataStream, StreamExecutionEnvironment}
import org.apache.flink.table.api.{Over, Table, Tumble}
import org.apache.flink.table.api.scala.StreamTableEnvironment
import org.apache.flink.table.functions.ScalarFunction


object T1_ScalarFunctions {

  def main(args: Array[String]): Unit = {

    val environment: StreamExecutionEnvironment = SensorReadingDataSource.environment
    val tableEnvironment: StreamTableEnvironment = SensorReadingDataSource.getTableEnvironment

    val sensorReadingDataStreamOne: DataStream[SensorReading] = SensorReadingDataSource.getDataSource("192.168.28.10", 7777)

    // 此处得到结论，如果DataStream里面的event类型是scala的case class。
    // 则表里面字段的名字默认为class字段的名字。
    // 但是最好自己重新写一遍名字，因为我们需要指定eventTime字段。
    //  因为在DataStream里面已经指定了使用timestamp字段做为水印的时间戳，所以此处再次指定其为Table里面的eventTime

    //    此处的表对象是一个inlined table。即没有注册的表
    val tableOne: Table = sensorReadingDataStreamOne
      .toTable(tableEnvironment, 'id, 'timestamp.rowtime, 'temperature)

    //如果要在sql里面使用，则必须将其注册
    tableEnvironment.registerFunction("mytoUpperCase", new MyScalar)

    tableEnvironment.sqlQuery(
      s"""
         |select id,mytoUpperCase(id),mytoUpperCase(id,'pdn')
         |from $tableOne
         |""".stripMargin)
      .toRetractStream[Row]
      .print()

    //    在tableApi里面使用的时候,则必须创建其对象
    val myScalar = new MyScalar
    tableOne.select('id, myScalar('id), myScalar('id, "aa")).toRetractStream[Row].print()

    environment.execute()
  }
}

class MyScalar extends ScalarFunction {
  def eval(s: String): String = {
    s.toUpperCase
  }

  //    可以重载多个
  def eval(s: String, name: String) = {
    s.toUpperCase + name
  }

  //    指定返回的值类型
  override def getResultType(signature: Array[Class[_]]): TypeInformation[_] = {
    Types.STRING
  }
}

//输入是一行数据，返回的是一行数据，但是返回的数据可以有多列
class MyMapFunction extends ScalarFunction {
  def eval(a: String): Row = {
    Row.of(a, "pre-" + a)
  }

  override def getResultType(signature: Array[Class[_]]): TypeInformation[_] =
    Types.ROW(Types.STRING, Types.STRING)
}



