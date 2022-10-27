package org.wikimedia.kafka2hdfs

import org.apache.flink.streaming.api.functions.sink.filesystem.BucketAssigner
import org.apache.flink.streaming.api.functions.sink.filesystem.bucketassigners.DateTimeBucketAssigner

import java.time.{Instant, LocalDateTime, ZoneId}

object CustomBucketAssigner extends DateTimeBucketAssigner[TimestampedKafkaRecord]{

  override def getBucketId(in: TimestampedKafkaRecord, context: BucketAssigner.Context): String = {
    val time = LocalDateTime.ofInstant(Instant.ofEpochMilli(in.getTimestamp), ZoneId.systemDefault())
    // NOTE: hm... I had never thought of this before: s"year=${time.getYear}/day=${time.getDayOfYear}"
    val x = s"year=${time.getYear}/month=${time.getMonthValue}/day=${time.getDayOfMonth}/hour=${time.getHour}"
    //println(in.toString)
    //println(x)
    x
  }
}
