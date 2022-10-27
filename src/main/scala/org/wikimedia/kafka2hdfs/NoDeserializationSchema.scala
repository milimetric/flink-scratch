package org.wikimedia.kafka2hdfs

import org.apache.flink.api.common.typeinfo.TypeInformation
import org.apache.flink.streaming.connectors.kafka.KafkaDeserializationSchema
import org.apache.kafka.clients.consumer.ConsumerRecord
import org.apache.kafka.common.record.TimestampType

class NoDeserializationSchema(timestampPath: String, timestampFormatStr: String) extends KafkaDeserializationSchema[TimestampedKafkaRecord] {

  val jsonTimestampExtractor = new JsonTimestampExtractor(timestampPath, timestampFormatStr)

  override def isEndOfStream(t: TimestampedKafkaRecord): Boolean = false

  override def deserialize(consumerRecord: ConsumerRecord[Array[Byte], Array[Byte]]): TimestampedKafkaRecord = {
    val timestamp = consumerRecord.timestampType() match {
      case TimestampType.CREATE_TIME => consumerRecord.timestamp()
      case _ => jsonTimestampExtractor.getRecordTimestamp(consumerRecord.value())
    }
    new TimestampedKafkaRecord(consumerRecord.value(), timestamp)
  }

  override def getProducedType: TypeInformation[TimestampedKafkaRecord] = TypeInformation.of(classOf[TimestampedKafkaRecord])
}
