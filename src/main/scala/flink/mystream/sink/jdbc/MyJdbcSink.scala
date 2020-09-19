package flink.mystream.sink.jdbc

import java.sql.{Connection, DriverManager, PreparedStatement}

import flink.mystream.beans.SensorReading
import org.apache.flink.configuration.Configuration
import org.apache.flink.streaming.api.functions.sink.{RichSinkFunction, SinkFunction}

/**
 * 此处实际上就是mysql的jdbc使用
 */
class MyJdbcSink extends RichSinkFunction[SensorReading] {

  var connect: Connection = _
  var insert: PreparedStatement = _
  var updateStmt: PreparedStatement = _


  override def open(parameters: Configuration): Unit = {
    super.open(parameters)
    connect = DriverManager.getConnection("jdbc:mysql://localhost:3306/mydb?serverTimezone=GMT%2B8", "root", "root")
    insert = connect.prepareStatement("INSERT INTO temperatures (sensor, temp) VALUES (?,?)")
    updateStmt= connect.prepareStatement("update temperatures set temp = ? where sensor = ?")
  }

  // 调用连接，执行 sql
  override def invoke(value: SensorReading, context: SinkFunction.Context[_]): Unit = {
//    即对于flink传递过来的每一条数据，先进行更新数据库的操作，如果数据库里面以前不存在此条数据，则更新的结果为0
//    此时则向数据库里面插入数据
    updateStmt.setDouble(1, value.temperature)
    updateStmt.setString(2, value.id)
    updateStmt.execute()

    if (updateStmt.getUpdateCount == 0) {
      insert.setString(1, value.id)
      insert.setDouble(2, value.temperature)
      insert.execute()
    }
  }

  override def close(): Unit = {
    updateStmt.close()
    insert.close()
    connect.close()
  }
}
