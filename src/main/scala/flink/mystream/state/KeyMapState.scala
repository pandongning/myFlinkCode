package flink.mystream.state

import org.apache.flink.api.common.functions.RichMapFunction
import org.apache.flink.api.common.state.{MapState, MapStateDescriptor}
import org.apache.flink.configuration.Configuration
import org.apache.flink.streaming.api.scala.{DataStream, StreamExecutionEnvironment, _}

object KeyMapState {
  def main(args: Array[String]): Unit = {
    System.setProperty("hadoop.home.dir", "C:\\Users\\pdn\\mySoft\\hadoop-2.7.2")
    System.setProperty("HADOOP_USER_NAME", "root")

    val env: StreamExecutionEnvironment = StreamExecutionEnvironment.getExecutionEnvironment

    val value: DataStream[String] = env.socketTextStream("LocalOne", 8888)

    val keyedStream: KeyedStream[(String, Int), String] = value.flatMap((line: String) => line.split(","))
      .map((word: String) => (word, 1))
      .keyBy((tuple: (String, Int)) => tuple._1)


    val sumRes: DataStream[(String, Int)] = keyedStream.sum(1)
    sumRes.print("sumRes")

    //下面的功能和上面的sum的功能一样，其对应key的数据每来一条，将更新其key对应的key
    keyedStream.map(new MyKeyCountMapper).print("KeyMapState")

    /**
     * 输入a,b
     * 输出如下
     * sumRes:2> (b,1)
     * sumRes:6> (a,1)
     * KeyMapState:6> (a,1)
     * KeyMapState:2> (b,1)
     *
     * 接着输入a,b,c,a
     * 输出如下
     * sumRes:4> (c,1)
     * sumRes:6> (a,2)
     * sumRes:2> (b,2)
     * sumRes:6> (a,3)
     * KeyMapState:2> (b,2)
     * KeyMapState:4> (c,1)
     * KeyMapState:6> (a,2)  //此处就可以看出，每次来一个记录，就会触发一次计算，向外输出一条记录，所以其结果也是需要不断的更新
     * KeyMapState:6> (a,3)
     */

    env.execute()

  }

  class MyKeyCountMapper extends RichMapFunction[(String, Int), (String, Int)] {

    var KeyMapState: MapState[String, Int] = _


    override def open(parameters: Configuration): Unit = {
      val keyMapState = new MapStateDescriptor("KeyMapState", classOf[String], classOf[Int])
      KeyMapState = getRuntimeContext.getMapState(keyMapState)

    }

    override def map(value: (String, Int)): (String, Int) = {

      if (!KeyMapState.contains(value._1)) {
        KeyMapState.put(value._1, 1)
      } else {
        KeyMapState.put(value._1, KeyMapState.get(value._1) + 1)
      }
      (value._1, KeyMapState.get(value._1))
    }
  }


}

