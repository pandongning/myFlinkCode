package flink.mystream.asyncIo.sync.t2_SyncGaoDe

import com.alibaba.fastjson.{JSON, JSONObject}
import flink.mystream.asyncIo.beans.ActivityBean
import flink.mystream.utils.FlinkKafkaUtil
import org.apache.flink.api.common.functions.RichMapFunction
import org.apache.flink.configuration.Configuration
import org.apache.flink.streaming.api.scala.DataStream
import org.apache.http.client.methods.{CloseableHttpResponse, HttpGet}
import org.apache.http.impl.client.{CloseableHttpClient, HttpClients}
import org.apache.http.util.EntityUtils
import org.apache.flink.streaming.api.scala._

object QueryActivityLocation {

  def main(args: Array[String]): Unit = {
    val line: DataStream[String] = FlinkKafkaUtil.createKafkaSoureStream(args)
    line.map(new GeoToActivityBeanFunction).print()

    FlinkKafkaUtil.getEnvironment.execute()
  }
}

class GeoToActivityBeanFunction extends RichMapFunction[String, ActivityBean] {

  var httpclient: CloseableHttpClient = _

  override def open(parameters: Configuration): Unit = {
    httpclient = HttpClients.createDefault()
  }

  override def map(value: String): ActivityBean = {
    val fields: Array[String] = value.split(",")
    //    u001,A1,2019-09-02 10:10:11,1,115.908923,39.267291
    val uid: String = fields(0)
    val aid: String = fields(1)
    val time: String = fields(2)
    val eventType: Int = fields(3).toInt
    val longitude: String = fields(4)
    val latitude: String = fields(5)


    var province: String = null

    val url: String = "https://restapi.amap.com/v3/geocode/regeo?key=4924f7ef5c86a278f5500851541cdcff&location=" + longitude + "," + latitude
    val httpGet: HttpGet = new HttpGet(url)
    val response: CloseableHttpResponse = httpclient.execute(httpGet)

    try {
      val status: Int = response.getStatusLine.getStatusCode
      if (status == 200) { //获取请求的json字符串
        val result: String = EntityUtils.toString(response.getEntity)
        //System.out.println(result);
        //转成json对象
        val jsonObj: JSONObject = JSON.parseObject(result)
        //获取位置信息
        val regeocode: JSONObject = jsonObj.getJSONObject("regeocode")
        if (regeocode != null && !regeocode.isEmpty) {
          val address: JSONObject = regeocode.getJSONObject("addressComponent")
          //获取省市区
          province = address.getString("province")
          //String city = address.getString("city");
          //String businessAreas = address.getString("businessAreas");
        }
      }
    } finally response.close()

    ActivityBean.of(uid, aid, "null", time, eventType, longitude.toDouble, latitude.toDouble, province)
  }

  override def close(): Unit = {
    httpclient.close()
  }

}
