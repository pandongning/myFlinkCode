package flink.mystream.connector

import org.apache.flink.streaming.api.scala.StreamExecutionEnvironment
import org.apache.flink.streaming.connectors.fs.{SequenceFileWriter, StringWriter}
import org.apache.flink.streaming.connectors.fs.bucketing.{BucketingSink, DateTimeBucketer}
import org.apache.hadoop.io.{IntWritable, Text}

object hdfs {
  def main(args: Array[String]): Unit = {
    val environment: StreamExecutionEnvironment = StreamExecutionEnvironment.getExecutionEnvironment

    val bucketingSink: BucketingSink[(IntWritable, Text)] = new BucketingSink[(IntWritable, Text)]("/base/path")
    bucketingSink.setBucketer(new DateTimeBucketer("yyyy-MM-dd--HHmm"))
    bucketingSink.setWriter(new StringWriter)
    bucketingSink.setBatchSize(1024 * 1024 * 400) // this is 400 MB,
    bucketingSink.setBatchRolloverInterval(20 * 60 * 1000); // this is 20 mins


    environment.execute("hdfs")
  }
}
