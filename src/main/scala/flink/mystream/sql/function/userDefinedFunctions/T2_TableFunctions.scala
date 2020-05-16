package flink.mystream.sql.function.userDefinedFunctions

import org.apache.flink.table.api.scala._
import org.apache.flink.api.scala._
import flink.mystream.beans.SensorReading
import flink.mystream.utils.SensorReadingDataSource
import org.apache.flink.api.common.typeinfo.{TypeInformation, Types}
import org.apache.flink.streaming.api.scala.{DataStream, StreamExecutionEnvironment}
import org.apache.flink.table.api.{Over, Table, Tumble}
import org.apache.flink.table.api.scala.StreamTableEnvironment
import org.apache.flink.table.functions.{ScalarFunction, TableFunction}
import org.apache.flink.types.Row

object T2_TableFunctions {

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

    tableEnvironment.registerFunction("mySplitOne", new MySplitTableFunction("_"))
    tableEnvironment.registerFunction("mySplittwo", new CustomTypeSplit())


    /**
     * 输入
     * sensor_1a_2b_3c,1585018991000,9.0
     * 输出
     * (true,sensor_1a_2b_3c,sensor,6)
     * (true,sensor_1a_2b_3c,1a,2)
     * (true,sensor_1a_2b_3c,2b,2)
     * (true,sensor_1a_2b_3c,3c,2)
     *
     */

    //    下面的x,y表示row类型里面的每一个字段
    tableEnvironment.sqlQuery(
      s"""
         |select id,x,y
         |from $tableOne,
         |LATERAL TABLE(mySplittwo(id)) as T(x,y)
         |""".stripMargin)
      .toRetractStream[Row]
      .print()


    tableEnvironment.sqlQuery(
      s"""
         |select id,word,length
         |from $tableOne,
         |LATERAL TABLE(mySplitOne(id)) as T(word,length)
         |""".stripMargin)
      .toRetractStream[Row]
      .print()


    //    tableApi的使用
    val split = new MySplitTableFunction("_")
    tableOne
      .joinLateral(split('id) as('word, 'length))
      .select('id, 'word, 'length)
      .toRetractStream[Row]
      .print()

    environment.execute()
  }

}

// Row用于指定返回的值类型
class CustomTypeSplit extends TableFunction[Row] {

  def eval(str: String): Unit = {
    str.split("_").foreach({ s =>
      val row = new Row(2)
      row.setField(0, s)
      row.setField(1, s.length)
      collect(row)
    })
  }
//指定返回的值类型
  override def getResultType: TypeInformation[Row] = {
    Types.ROW(Types.STRING, Types.INT)
  }
}

//(String, Int)用于指定返回的一行的类型
class MySplitTableFunction(separator: String) extends TableFunction[(String, Int)] {

  //  collect用于输出行
  def eval(str: String) = {
    str.split(separator).foreach(x => collect((x, x.length)))
  }


  //  支持可变长参数
  def eval(args: String*): Unit = {
  }

  //  支持方法重载
  def eval(argsOne: String, argsTwo: Double) = {
  }


}
