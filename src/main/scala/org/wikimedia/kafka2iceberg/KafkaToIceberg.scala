package org.wikimedia.kafka2iceberg

import org.apache.flink.api.common.eventtime.WatermarkStrategy
import org.apache.flink.api.java.utils.ParameterTool
import org.apache.flink.api.scala.createTypeInformation
import org.apache.flink.configuration
import org.apache.flink.streaming.api.scala.StreamExecutionEnvironment
import org.apache.flink.streaming.connectors.kafka.table.KafkaDynamicTableFactory
import org.apache.flink.table.api.{DataTypes, PlanReference, TableEnvironment, TableSchema}
import org.apache.flink.table.data.binary.{BinaryRowData, NestedRowData}
import org.apache.flink.table.data.columnar.ColumnarRowData
import org.apache.flink.table.data.{DecimalData, GenericRowData, RowData, StringData}
import org.apache.flink.table.types.logical.VarCharType
import org.apache.flink.types.{Row, RowKind}
import org.apache.hadoop.conf.Configuration
import org.apache.iceberg.Table
import org.apache.iceberg.catalog.{Catalog, Namespace, TableIdentifier}
import org.apache.iceberg.flink.sink.FlinkSink
import org.apache.iceberg.flink.{CatalogLoader, TableLoader}
import org.apache.iceberg.util.JsonUtil.factory
import org.wikimedia.eventutilities.flink.stream.EventDataStreamFactory
import org.wikimedia.eventutilities.flink.table.EventTableDescriptorBuilder
import org.apache.flink.table.api.Expressions._
import org.apache.flink.table.api.bridge.scala.StreamTableEnvironment

import java.util
import scala.collection.JavaConverters.{mapAsJavaMapConverter, seqAsJavaListConverter}

object KafkaToIceberg {
  private val log = org.slf4j.LoggerFactory.getLogger(classOf[Object])

  def main(args: Array[String]): Unit = {
    val toMerge = ParameterTool.fromArgs(args)
    val propertyFile = toMerge.get("properties", "default.properties")
    val parameters = toMerge.mergeWith(ParameterTool.fromPropertiesFile(propertyFile))
    val props = parameters.getProperties

    val kafkaBootstrapServers = props.getProperty("bootstrap.servers")
    val kafkaGroupId = props.getProperty("group.id")
    // database, page_id, rev_id
    val revisionCreateStreamName = props.getProperty("topics.revision.create")
    // database, page_id, rev_id, visibility.comment, visibility.text, visibility.user
    val revisionVisibilityStreamName = props.getProperty("topics.revision.visibility")
    // database, page_id
    val pageDeleteStreamName = props.getProperty("topics.page.delete")
    // database, page_id
    val pageUndeleteStreamName = props.getProperty("topics.page.undelete")

    val kafkaSourceRoutes = Option(props.getProperty("KAFKA_SOURCE_ROUTES"))
    val kafkaClientRoutes = Option(props.getProperty("KAFKA_CLIENT_ROUTES"))

    val eventSchemaBaseUris = props.getProperty("EVENT_SCHEMA_BASE_URIS").split(",")
    val eventStreamConfigUri = props.getProperty("EVENT_STREAM_CONFIG_URI")
    val kafkaHttpClientRoutes = (kafkaSourceRoutes, kafkaClientRoutes) match {
      case (Some(source), Some(client)) =>
        Option(
          source.split(",")
            .zip(client.split(","))
            .toMap.asJava
        )
      case _ => None
    }

    val tableBuilder = EventTableDescriptorBuilder.from(
      eventSchemaBaseUris.toList.asJava,
      eventStreamConfigUri,
    )

    val env = StreamExecutionEnvironment.getExecutionEnvironment
    val tenv = StreamTableEnvironment.create(env)
    val revisionCreateTable = tenv.from(
      tableBuilder
        .eventStream(revisionCreateStreamName)
        .setupKafka(kafkaBootstrapServers, kafkaGroupId)
        .build()
    )

    val hadoopConf = new Configuration(true)
    hadoopConf.set("fs.hdfs.impl", classOf[org.apache.hadoop.hdfs.DistributedFileSystem].getName)

    val hiveCatalogName = props.getProperty("hive.catalog")
    val hiveDatabase = props.getProperty("hive.database")
    val hiveRevisionTable = props.getProperty("hive.table.revision")
    val catalogProperties = Map(
      "uri" -> props.getProperty("hive.uri"),
      "warehouse" -> props.getProperty("hive.warehouse"),
    ).asJava

    val loader = CatalogLoader.hive(
      hiveCatalogName,
      hadoopConf,
      catalogProperties,
    )
    val revisionIcebergTableLoader = TableLoader.fromCatalog(loader, TableIdentifier.of(hiveDatabase, hiveRevisionTable))
    // val unifiedRevisionTable = revisionIcebergTableLoader.loadTable()
    FlinkSink
      .forRow(tenv.toDataStream(revisionCreateTable).javaStream, revisionCreateTable.getSchema)
      .tableLoader(revisionIcebergTableLoader)
      .append()

    env.execute()
  }
}
