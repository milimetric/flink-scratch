package org.wikimedia.kafka2iceberg

import org.apache.flink.api.java.utils.ParameterTool
import org.apache.flink.streaming.api.scala.StreamExecutionEnvironment
import org.apache.flink.table.api.Expressions._
import org.apache.flink.table.api.{DataTypes, Schema, TableDescriptor}
import org.apache.hadoop.conf.Configuration
import org.apache.iceberg.flink.FlinkCatalogFactory
import org.wikimedia.eventutilities.flink.table.EventTableDescriptorBuilder
import org.apache.flink.table.api.bridge.scala.StreamTableEnvironment
import org.apache.flink.table.catalog.ObjectPath

import scala.collection.JavaConverters._

object KafkaToIceberg {
  private val log = org.slf4j.LoggerFactory.getLogger(classOf[Object])

  def main(args: Array[String]): Unit = {
    val toMerge = ParameterTool.fromArgs(args)
    val propertyFile = toMerge.get("properties", "default.properties")
    val parameters = toMerge.mergeWith(ParameterTool.fromPropertiesFile(propertyFile))
    val props = parameters.getProperties

    val kafkaBootstrapServers = props.getProperty("bootstrap.servers", "kafka-jumbo1007.eqiad.wmnet:9092")
    val kafkaGroupId = props.getProperty("group.id", "otto_iceberg1")

    val eventSchemaBaseUris = props.getProperty(
        "EVENT_SCHEMA_BASE_URIS",
        "https://schema.wikimedia.org/repositories/primary/jsonschema,https://schema.wikimedia.org/repositories/secondary/jsonschema",
    ).split(",")
    val eventStreamConfigUri = props.getProperty(
        "EVENT_STREAM_CONFIG_URI",
        "https://meta.wikimedia.org/w/api.php"
    )

      val hadoopConf = new Configuration(true)
      hadoopConf.set("fs.hdfs.impl", classOf[org.apache.hadoop.hdfs.DistributedFileSystem].getName)

      val pageCommentTableName = props.getProperty("page_comment_table")
//      val catalogProperties = Map(
//          "uri" -> props.getProperty("hive.uri"),
//          "warehouse" -> props.getProperty("hive.warehouse"),
//      ).asJava


      val env = StreamExecutionEnvironment.getExecutionEnvironment
      val tableEnv = StreamTableEnvironment.create(env)

      val tableDescriptorBuilder = EventTableDescriptorBuilder.from(
          eventSchemaBaseUris.toList.asJava,
          eventStreamConfigUri
      )

      val streamTable = tableEnv.from(
          tableDescriptorBuilder
              .eventStream("mediawiki.revision-create")
              .setupKafka(
                  kafkaBootstrapServers,
                  kafkaGroupId
              )
              .build()
      )

      val icebergHiveCatalogProps = Map(
        "catalog-type" -> "hive",
        "uri" -> "thrift://analytics-hive.eqiad.wmnet:9083",
        "hadoop-conf-dir" -> "/etc/hadoop/conf",
        "hive-conf-dir" -> "/etc/hive/conf",
        "warehouse" -> "/user/hive/warehouse"
      )

      val flinkIcebergHiveCatalog = new FlinkCatalogFactory().createCatalog(
          "analytics_hive", icebergHiveCatalogProps.asJava
      )
      tableEnv.registerCatalog("analytics_hive", flinkIcebergHiveCatalog)


      val sinkTableName = "analytics_hive.otto." + pageCommentTableName

      if (!flinkIcebergHiveCatalog.tableExists(ObjectPath.fromString(sinkTableName))) {
          val icebergTableSchema = Schema.newBuilder()
              .column("wiki_id", DataTypes.STRING().notNull())
              .column("page_id", DataTypes.BIGINT().notNull())
              .column("rev_id", DataTypes.BIGINT().notNull())
              .column("comment", DataTypes.STRING())
              .primaryKey("wiki_id", "page_id")
              .build()

          val icebergRevisionTableDescriptor = TableDescriptor
              .forManaged()
              .schema(icebergTableSchema)
              .format("parquet")
              .option("format-version", "2")
              .option("write.upsert.enabled", "true")
              .build()

          tableEnv.createTable(sinkTableName , icebergRevisionTableDescriptor)
      }

      val selectResult = streamTable.select(
          $("database").as("wiki_id"),
          $("page_id"),
          $("rev_id"),
          $("comment")
      )

      val pipeline = selectResult.insertInto(sinkTableName)
      pipeline.printExplain()
      pipeline.execute()


  }
}
