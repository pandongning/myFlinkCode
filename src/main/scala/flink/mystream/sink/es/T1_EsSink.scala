package flink.mystream.sink.es

import java.net.{InetAddress, InetSocketAddress}
import java.util

import flink.mystream.beans.SensorReading
import org.apache.flink.api.common.functions.RuntimeContext
import org.apache.flink.streaming.api.scala.{DataStream, StreamExecutionEnvironment}
import org.apache.flink.api.scala._
import org.apache.flink.streaming.connectors.elasticsearch.{ElasticsearchSinkFunction, RequestIndexer}
import org.apache.flink.streaming.connectors.elasticsearch5.ElasticsearchSink
import org.elasticsearch.action.index.IndexRequest
import org.elasticsearch.client.Requests

object T1_EsSink {
  def main(args: Array[String]): Unit = {
    val environment: StreamExecutionEnvironment = StreamExecutionEnvironment.getExecutionEnvironment
    val sourceDataStream: DataStream[String] = environment.readTextFile("src/main/resources/sensor.txt")

    val sensorReadingDataStream: DataStream[SensorReading] = sourceDataStream.map(
      line => {
        val dataArray: Array[String] = line.split(",")
        SensorReading(dataArray(0), dataArray(1).trim.toLong, dataArray(2).trim.toDouble)
      }
    )

    sensorReadingDataStream.print()

    val config: util.HashMap[String, String] = new util.HashMap[String, String]()
    config.put("cluster.name", "my-elasticsearch")
    config.put("bulk.flush.max.actions", "1")

    val transportAddresses: util.ArrayList[InetSocketAddress] = new util.ArrayList[InetSocketAddress]()
    transportAddresses.add(new InetSocketAddress(InetAddress.getByName("127.0.0.1"), 9300))

    val elasticsearchSink: ElasticsearchSink[SensorReading] = new ElasticsearchSink(config, transportAddresses, new ElasticsearchSinkFunction[SensorReading] {
      override def process(sensorReading: SensorReading, runtimeContext: RuntimeContext, requestIndexer: RequestIndexer): Unit = {
        val json: util.HashMap[String, String] = new util.HashMap[String, String]()
        json.put("sensor_id", sensorReading.id)
        json.put("temperature", sensorReading.temperature.toString)
        json.put("ts", sensorReading.timestamp.toString)

        val indexRequest: IndexRequest = Requests.indexRequest()
          .index("flinksensorreading")
          .`type`("sensorReading")
          .source(json)

        requestIndexer.add(indexRequest)
        println("data saved.")
      }
    })

    sensorReadingDataStream.addSink(elasticsearchSink)

    environment.execute()

  }
}
