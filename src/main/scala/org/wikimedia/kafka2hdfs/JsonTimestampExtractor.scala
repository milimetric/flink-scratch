package org.wikimedia.kafka2hdfs

import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.databind.ObjectMapper

import java.io.IOException
import java.text.SimpleDateFormat
import java.time.{LocalDateTime, ZoneId}

object TimestampFormat extends Enumeration {
  type TimestampFormat = Value
  val unix, unix_seconds, unix_milliseconds, ISO_8601, CUSTOM = Value
  def safeWithName (s: String): Value = {
    values.find(_.toString == s).getOrElse(CUSTOM)
  }
}

/**
 * A utility class parsing JSON from byte[] and extracting a timestamp from the data.
 *
 * If a record doesn't contain the specified field, or if no field is specified, the current timestamp will be used.
 *
 * Accepted formats are:
 * <ul>
 * <li>unix (same as unix_seconds)</li>
 * <li>unix_seconds</li>
 * <li>unix_milliseconds</li>
 * <li>ISO_8601</li>
 * <li>CUSTOM - Using the timestampCustomFormat parameter (can be empty for other values)</li>
 * </ul>
 *
 */
class JsonTimestampExtractor(timestampPointer: String, timestampFormatStr: String) extends Serializable {
  private val log = org.slf4j.LoggerFactory.getLogger(classOf[Nothing])

  val cachingMapper = new ObjectMapper()
  private val pointer = "/" + timestampPointer.replaceAll("\\.", "/")
  private val format = TimestampFormat.safeWithName(timestampFormatStr)
  private val parser = format match {
    case TimestampFormat.CUSTOM => Option(new SimpleDateFormat(timestampFormatStr))
    case _ => None
  }

  def getRecordTimestamp(payload: Array[Byte]): Long = {
    val ifNotFound = System.currentTimeMillis()
    try {
      val value = cachingMapper.readTree(payload).at(pointer)

      if (value.canConvertToLong) {
        val found = value.asLong()
        return format match {
          case TimestampFormat.unix => found * 1000
          case TimestampFormat.unix_seconds => found * 1000
          case TimestampFormat.unix_milliseconds => found
          case anythingElse => throw new Exception(s"$anythingElse is not a valid format for this numerical timestamp value")
        }
      } else if (value.isTextual) {
        val found = value.asText()
        return format match {
          case TimestampFormat.ISO_8601 => LocalDateTime.parse(found).atZone(ZoneId.systemDefault()).toInstant.toEpochMilli
          case TimestampFormat.CUSTOM => parser.get.parse(found).getTime
          case anythingElse => throw new Exception(s"$anythingElse is not a valid format for this textual timestamp value")
        }
      }
    } catch {
      case e: IOException => log.warn(s"Couldn't parse the json payload for timestamp extraction: $payload", e)
      case e: Exception => log.warn(s"Failed to extract timestamp from json payload: $payload", e)
    }
    ifNotFound
  }
}