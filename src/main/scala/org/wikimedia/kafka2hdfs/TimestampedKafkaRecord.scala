package org.wikimedia.kafka2hdfs

class TimestampedKafkaRecord (value: Array[Byte], timestamp: Long) {
  def getTimestamp: Long = timestamp

  override def toString: String = new String(value)
}