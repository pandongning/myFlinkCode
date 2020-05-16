package flink.mystream.asyncIo.Async

import java.util.concurrent.Future

import flink.mystream.asyncIo.beans.ActivityBean
import flink.mystream.utils.FlinkKafkaUtil
import org.apache.flink.configuration.Configuration
import org.apache.flink.streaming.api.scala.DataStream
import org.apache.flink.streaming.api.scala.async.{ResultFuture, RichAsyncFunction}
import org.apache.http.HttpResponse
import org.apache.http.client.config.RequestConfig
import org.apache.http.client.methods.HttpGet
import org.apache.http.impl.nio.client.{CloseableHttpAsyncClient, HttpAsyncClients}

object AsyncQueryActivityLocation {
  def main(args: Array[String]): Unit = {
    val line: DataStream[String] = FlinkKafkaUtil.createKafkaSoureStream(args)
    //    AsyncDataStream.unorderedWait(line,)
  }
}

class AsyncGeoToActivityBeanFunction extends RichAsyncFunction[String, ActivityBean] {
  // 不参与 反序列化。因为其不需要被持久化和ck
  @transient
  var httpclient: CloseableHttpAsyncClient = _

  override def open(parameters: Configuration): Unit = {
    val requestConfig: RequestConfig = RequestConfig
      .custom()
      .setSocketTimeout(3000)
      .setConnectTimeout(3000) build()

    httpclient = HttpAsyncClients.custom.setMaxConnTotal(20).setDefaultRequestConfig(requestConfig).build

    httpclient.start()
  }

  override def asyncInvoke(input: String, resultFuture: ResultFuture[ActivityBean]): Unit = {

    val fields: Array[String] = input.split(",")
    val uid: String = fields(0)
    val aid: String = fields(1)
    val time: String = fields(2)
    val eventType: Int = fields(3).toInt
    val longitude: String = fields(4)
    val latitude: String = fields(5)

    val url: String = "https://restapi.amap.com/v3/geocode/regeo?key=4924f7ef5c86a278f5500851541cdcff&location=" + longitude + "," + latitude

    val httpGet: HttpGet = new HttpGet(url)

    val future: Future[HttpResponse] = httpclient.execute(httpGet, null)


  }
}

