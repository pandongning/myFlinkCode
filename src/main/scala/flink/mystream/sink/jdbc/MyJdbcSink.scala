package flink.mystream.sink.jdbc

import java.sql.{Connection, DriverManager, PreparedStatement}

import flink.mystream.beans.SensorReading
import org.apache.flink.configuration.Configuration
import org.apache.flink.streaming.api.functions.sink.{RichSinkFunction, SinkFunction}

class MyJdbcSink extends RichSinkFunction[SensorReading] {

  var connect: Connection = _
  var insert: PreparedStatement = _
  var updateStmt: PreparedStatement = _


  override def open(parameters: Configuration): Unit = {
    super.open(parameters)
    connect = DriverManager.getConnection("jdbc:mysql://localhost:3306/mydb?serverTimezone=GMT%2B8", "root", "root")
    insert = connect.prepareStatement("INSERT INTO temperatures (sensor, temp) VALUES (?,?)")
    updateStmt = connect.prepareStatement("update temperatures set temp = ? where sensor = ?")
  }

  // 调用连接，执行 sql
  override def invoke(value: SensorReading, context: SinkFunction.Context[_]): Unit = {
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
