package flink.mystream.beans

import org.apache.flink.table.api.scala._

case class MergeBean(id: String, timestamp: Long, temperature: Double, idTwo: String, timestampTwo: Long, name: String)