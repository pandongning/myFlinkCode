package flink.mystream.sql.connector

import org.apache.flink.api.common.restartstrategy.RestartStrategies
import org.apache.flink.table.api.scala._
import org.apache.flink.api.scala._
import org.apache.flink.streaming.api.CheckpointingMode
import org.apache.flink.streaming.api.environment.CheckpointConfig.ExternalizedCheckpointCleanup
import org.apache.flink.streaming.api.scala.{DataStream, StreamExecutionEnvironment}
import org.apache.flink.table.api.{DataTypes, EnvironmentSettings, Table}
import org.apache.flink.table.api.scala.StreamTableEnvironment
import org.apache.flink.table.descriptors.{Csv, Json, Kafka, Rowtime, Schema}
import org.apache.flink.types.Row

object T1_KafkaConnector_RegularJoin {
  def main(args: Array[String]): Unit = {

    val environment: StreamExecutionEnvironment = StreamExecutionEnvironment.getExecutionEnvironment

    environment.enableCheckpointing(60000L)
    environment.getCheckpointConfig.setCheckpointingMode(CheckpointingMode.EXACTLY_ONCE)
    environment.getCheckpointConfig.setCheckpointTimeout(10000L)
    environment.getCheckpointConfig.setFailOnCheckpointingErrors(false)
    environment.getCheckpointConfig.setMaxConcurrentCheckpoints(1)
    //    当任务最终失败或者被取消的时候，保留外部的ck。这样则可以自己检查任务，然后手动的恢复
    environment.getCheckpointConfig.enableExternalizedCheckpoints(ExternalizedCheckpointCleanup.RETAIN_ON_CANCELLATION)

    import org.apache.flink.runtime.state.filesystem.FsStateBackend
    environment.setStateBackend(new FsStateBackend("file:///D:/checkpoints"))

    //    失败之后自动重启3次，每次的间隔设置为120s。超过3次则直接判定任务失败。
    environment.setRestartStrategy(RestartStrategies.fixedDelayRestart(3, org.apache.flink.api.common.time.Time.seconds(120)))


    val environmentSettings: EnvironmentSettings = EnvironmentSettings.newInstance().useBlinkPlanner().inStreamingMode().build()
    val tableEnvironment: StreamTableEnvironment = StreamTableEnvironment.create(environment, environmentSettings)

    tableEnvironment
      .connect(new Kafka()
        .version("universal")
        .topic("first")
        .property("zookeeper.connect", "LocalOne:2181")
        .property("bootstrap.servers", "LocalOne:9092")
        .startFromLatest()) //如果第一次将所有的数据打到binlog里面。则会采集全量的。
      .withFormat(new Csv().deriveSchema())
      .withSchema(new Schema()
        .field("id", DataTypes.STRING)
        .field("timestamp", DataTypes.BIGINT).rowtime(new Rowtime().timestampsFromField("timestamp").watermarksPeriodicBounded(1000L))
        .field("temper", DataTypes.DOUBLE)
      )
      .createTemporaryTable("inputTableOne")

    //    val table: Table = tableEnvironment.sqlQuery("select * from inputTableOne")

    //    table.printSchema()
    //
    //    val resultSqlTable: Table = tableEnvironment.sqlQuery("select id, count(id) as cnt, avg(temper) as avgTemp, tumble_end(timestamp, interval '2' second) " + "from inputTableOne group by id, tumble(timestamp, interval '2' second)")
    //    resultSqlTable.toRetractStream[Row].print()

    tableEnvironment
      .connect(new Kafka()
        .version("universal")
        .topic("second")
        .property("zookeeper.connect", "LocalOne:2181")
        .property("bootstrap.servers", "LocalOne:9092")
        .startFromLatest())
      .withFormat(new Csv().deriveSchema())
      .withSchema(new Schema()
        .field("id", DataTypes.STRING)
        .field("ts", DataTypes.BIGINT).rowtime(new Rowtime().timestampsFromField("ts").watermarksPeriodicBounded(1000L))
        .field("name", DataTypes.STRING())
      )
      .createTemporaryTable("inputTableTwo")


    //    此处做的是两个表的Regular Join，所以其会保留所有读取的历史数据
    /**
     * 输入的过程如下
     * 给first的topic输入
     * sensor_3,1599990790000,1.0
     * 此时因为second的topic里面还没有其对应的维度表数据，所以其输出为
     * (true,sensor_3,1599990790000,1.0,null,null,null)
     *
     * second的topic输入
     * sensor_3,1599990790000,cc
     * 此时因为有了维度数据。所以此时会得到下面的输出。false表示以前的结果作废
     * (false,sensor_3,1599990790000,1.0,null,null,null)
     * (true,sensor_3,1599990790000,1.0,sensor_3,1599990790000,cc)
     *
     * 此时给first的topic输入
     * sensor_3,1599990790000,2.0
     * 则得到的输出如下。
     * 4> (true,sensor_3,1599990790000,2.0,sensor_3,1599990790000,cc)
     *
     *
     * 但是此时如果改变维度表的数据
     * 给second topic输入
     * sensor_3,1599990790000,ee
     * 则此时得到的输出如下。可以看出其保留了双流里面的全部历史数据，任何一个流的数据输入都会驱动历史数据的变化。
     * 而且任何一个流的输入都会改变流的输出结果，所以其是双流驱动的
     * (true,sensor_3,1599990790000,2.0,sensor_3,1599990790000,ee)
     * (true,sensor_3,1599990790000,1.0,sensor_3,1599990790000,ee)
     *
     * 此时再fisrt输入
     * sensor_3,1599990790000,3.0
     * 可以看见下面的输出。即可以看到新来的一条数据关联到了两条维表的 数据。因为此时second的toipc里面就有两条对应的sensor_3
     * 而且regular进行的就是将双流里面的数据全部保存的，所以此时inputTableOne里面的一条数据可以关联到inputTableTwo里面的两条数据
     * 从此看来如果要使用regular_join，则必须是筛选出true打头的，然后将其update到外部的存储里面即可
     * 4> (true,sensor_3,1599990790000,3.0,sensor_3,1599990790000,ee)
     * 4> (true,sensor_3,1599990790000,3.0,sensor_3,1599990790000,cc)
     *
     *
     * 再次second输入
     * sensor_3,1599990790000,ee
     * 则得到。可以看出，其做的是全量的join。
     * 此处容易引起自己误解的是，如果传递是mysql_binlog的解析数据，次数则对于维度的数据，应该属于变更，而不是直接插入，如果是直接插入，则
     * 此处就是多了一条相同的维度数据
     * 4> (true,sensor_3,1599990790000,4.0,sensor_3,1599990790000,ee)
     * 4> (true,sensor_3,1599990790000,4.0,sensor_3,1599990790000,ee)
     * 4> (true,sensor_3,1599990790000,4.0,sensor_3,1599990790000,cc)
     *
     */


    /**
     * 可以看到regular_join保留了所有的历史数据
     * 所以此时此时需要对状态配置存活的时间
     * import org.apache.flink.table.api.TableConfig
     * // obtain query configuration from TableEnvironment// obtain query configuration from TableEnvironment
     * val tConfig: TableConfig = tableEnv.getConfig
     * // set query parameters。第一个参数是最小的时间，第二个参数是最大的时间
     * tConfig.setIdleStateRetentionTime(Time.hours(12), Time.hours(24))
     */
    val table1: Table = tableEnvironment
      .sqlQuery("select * from inputTableOne t1 left join inputTableTwo t2 on t1.id=t2.id")


    //    下面的聚合也可以看出其是在全量的数据之上进行的聚合
    val table2: Table = tableEnvironment
      .sqlQuery("select sum(t1.temper),t2.name from inputTableOne t1 left join inputTableTwo t2 on t1.id=t2.id group by t2.name")
    val result: DataStream[(Boolean, Row)] = table1.toRetractStream[Row]
    result.print()

    table2.printSchema()

    //    val value: DataStream[(Boolean, AnyRef, AnyRef, AnyRef, AnyRef)] = result
    //      .map((x: (Boolean, Row)) => (x._1, x._2.getField(0), x._2.getField(1), x._2.getField(2), x._2.getField(5)))
    //
    //    tableEnvironment.fromDataStream(value, 'is_true, 'id, 'ts.rowtime, 'temp, 'name).printSchema()

    //    table.printSchema()


    environment.execute()


  }
}
