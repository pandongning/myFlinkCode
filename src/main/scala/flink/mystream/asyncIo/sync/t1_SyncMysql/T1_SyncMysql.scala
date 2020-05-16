package flink.mystream.asyncIo.sync.t1_SyncMysql

import java.sql.{Connection, DriverManager, PreparedStatement, ResultSet}

import flink.mystream.asyncIo.beans.ActivityBean
import flink.mystream.utils.FlinkKafkaUtil
import org.apache.flink.api.common.functions.RichMapFunction
import org.apache.flink.configuration.Configuration
import org.apache.flink.streaming.api.scala.DataStream
import org.apache.flink.streaming.api.scala._

/**
 * 对应duoyi的第一个需求
 * 将kafka收集到的每一条数据，实时的关联mysql，获取其维度信息
 */

object T1_SyncMysql {

  def main(args: Array[String]): Unit = {

    val line: DataStream[String] = FlinkKafkaUtil.createKafkaSoureStream(args)
    val value: DataStream[ActivityBean] = line.map(new LineToActivityBean)
    value.print()

    FlinkKafkaUtil.getEnvironment.execute()
  }
}

class LineToActivityBean extends RichMapFunction[String, ActivityBean] {

  var connection: Connection = _

  override def open(parameters: Configuration): Unit = {
    connection = DriverManager.getConnection("jdbc:mysql://localhost:3306/mydb?serverTimezone=GMT%2B8", "root", "root")

  }


  override def map(value: String): ActivityBean = {

    val fields: Array[String] = value.split(",")
    //    u001,A1,2019-09-02 10:10:11,1,北京市
    val uid: String = fields(0)
    val aid: String = fields(1)
    val time: String = fields(2)
    val eventType: Int = fields(3).toInt
    val province: String = fields(4)
    var name: String = null

    val preparedStatement: PreparedStatement = connection.prepareStatement("select name from t_activities where id=?")
    preparedStatement.setString(1, aid)
    val resultSet: ResultSet = preparedStatement.executeQuery()

    while (resultSet.next()) {
      name = resultSet.getString(1);
    }
    preparedStatement.close()

    ActivityBean.of(uid, aid, name, time, eventType, province)
  }

  override def close(): Unit = {
    super.close()
    connection.close()
  }


}


