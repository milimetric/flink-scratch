/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.wikimedia.kafka2hdfs

import org.apache.flink.api.common.serialization.SimpleStringEncoder
import org.apache.flink.api.java.utils.ParameterTool
import org.apache.flink.api.scala.createTypeInformation
import org.apache.flink.connector.file.sink.FileSink
import org.apache.flink.core.fs.Path
import org.apache.flink.streaming.api.functions.sink.filesystem.rollingpolicies.DefaultRollingPolicy
import org.apache.flink.streaming.api.scala.StreamExecutionEnvironment
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer

import java.util.concurrent.TimeUnit

/**
 * Skeleton for a Flink Streaming Job.
 *
 * For a tutorial how to write a Flink streaming application, check the
 * tutorials and examples on the <a href="https://flink.apache.org/docs/stable/">Flink Website</a>.
 *
 * To package your application into a JAR file for execution, run
 * 'mvn clean package' on the command line.
 *
 * If you change the name of the main class (with the public static void main(String[] args))
 * method, change the respective entry in the POM.xml file (simply search for 'mainClass').
 */
object KafkaToHdfs {

  def main(args: Array[String]): Unit = {
    val toMerge = ParameterTool.fromArgs(args)
    val propertyFile = toMerge.get("properties", "default.properties")
    val parameters = toMerge.mergeWith(ParameterTool.fromPropertiesFile(propertyFile))
    val properties = parameters.getProperties

    val env = StreamExecutionEnvironment.getExecutionEnvironment

    // ***************
    // source
    val consumer = new FlinkKafkaConsumer[TimestampedKafkaRecord](
      parameters.get("topic"),
      new NoDeserializationSchema(parameters.get("record.timestamp.pointer"), parameters.get("record.timestamp.format")),
      properties // TODO: limit this to only the props the consumer expects, otherwise it throws warnings
    )
    consumer.setStartFromEarliest()
    val stream = env.addSource(consumer)

    // ***************
    // sink
    val path = new Path(parameters.get("output.path"))
    val sink = FileSink
      .forRowFormat(path, new SimpleStringEncoder[TimestampedKafkaRecord]("UTF-8"))
      .withBucketAssigner(CustomBucketAssigner)
      .withRollingPolicy(
        DefaultRollingPolicy.builder()
          .withRolloverInterval(TimeUnit.MINUTES.toMillis(5))
          .withInactivityInterval(TimeUnit.MINUTES.toMillis(60))
          .withMaxPartSize(1024 * 1024)
          .build()
      )
      .build()

    // ***************
    // connect and execute
    stream.sinkTo(sink)
    env.execute("Flink Streaming Scala API Skeleton")
  }
}
