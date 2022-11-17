package org.wikimedia.kafka2iceberg

import org.apache.flink.api.java.utils.ParameterTool
import org.apache.flink.streaming.api.scala.StreamExecutionEnvironment
import org.apache.flink.table.api.{DataTypes, Schema, TableDescriptor}
import org.apache.hadoop.conf.Configuration
import org.apache.iceberg.flink.FlinkCatalogFactory
import org.wikimedia.eventutilities.flink.table.EventTableDescriptorBuilder
import org.apache.flink.table.api.Expressions._
import org.apache.flink.table.api.bridge.scala.StreamTableEnvironment
import org.apache.flink.table.catalog.ObjectPath

import java.util
import scala.collection.JavaConverters.{mapAsJavaMapConverter, seqAsJavaListConverter}

/* NOTES from great search meeting helping me with the build
# one idea: use yarn instead of standalone cluster
sudo -u analytics-search kerberos-run-command analytics-search sh -c 'HADOOP_CLASSPATH="`hadoop classpath`" ./bin/yarn-session.sh -tm 2000m -jm 1600m -s 4 -nm "WDQS Streaming Updater (T302396)"'

# use discovery parent pom and all the maven inspection tools from there
mvn sortpom:sort
mvn clean verify
mvn dependency:tree
 */

/*
export HADOOP_CLASSPATH=`$HADOOP_HOME/bin/hadoop classpath`
export HIVE_CONF_DIR=/etc/hive/conf
export HADOOP_CONF_DIR=/etc/hadoop/conf
export HBASE_CONF_DIR=/etc/hbase/conf
./bin/start-cluster.sh
./bin/flink run --detached ../flink-scratch-0.1.jar --properties ../revisions-to-iceberg.properties

 */
object KafkaToIceberg {
  private val log = org.slf4j.LoggerFactory.getLogger(classOf[Object])

  log.warn(System.getProperty("java.class.path"))

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

    val hiveCatalogName = props.getProperty("hive.catalog")
    val hiveDatabase = props.getProperty("hive.database")
    val hiveRevisionTable = props.getProperty("hive.table.revision")
    val hiveUri = props.getProperty("hive.uri")
    val hiveWarehouse = props.getProperty("hive.warehouse")

    val sinkTableName = props.getProperty("sink.table.name")
    val catalogName = "analytics_hive"

    val icebergHiveCatalogProps = Map(
      //"type" -> "iceberg",
      "catalog-type" -> "hive",
      "uri" -> hiveUri,
      "warehouse" -> hiveWarehouse,
      "hadoop-conf-dir" -> "/etc/hadoop/conf",
      "hive-conf-dir" -> "/etc/hive/conf",
    )

    val hadoopConf = new Configuration(true)
    // hadoopConf.set("fs.abstractFileSystem.hdfs.impl", classOf[org.apache.hadoop.hdfs.DistributedFileSystem].getName)
    hadoopConf.set("fs.hdfs.impl", classOf[org.apache.hadoop.hdfs.DistributedFileSystem].getName)

    val env = StreamExecutionEnvironment.getExecutionEnvironment
    val tableEnv = StreamTableEnvironment.create(env)

    val tableDescriptorBuilder = EventTableDescriptorBuilder.from(
      eventSchemaBaseUris.toList.asJava,
      eventStreamConfigUri
    )

    val revisionCreateStreamTable = tableEnv.from(
      tableDescriptorBuilder
        .option("json.timestamp-format.standard", "ISO-8601")
        .eventStream(revisionCreateStreamName)
        .setupKafka(
          kafkaBootstrapServers,
          kafkaGroupId
        )
        .build()
    )

    val flinkTableHiveCatalog = new FlinkCatalogFactory()
      .createCatalog(
        catalogName, icebergHiveCatalogProps.asJava
      )
    tableEnv.registerCatalog(catalogName, flinkTableHiveCatalog)

    val unifiedRevisionTableName = sinkTableName
    val unifiedRevisionTableNameWithCatalog = catalogName + "." + sinkTableName

    if (!flinkTableHiveCatalog.tableExists(ObjectPath.fromString(unifiedRevisionTableName))) {
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

      tableEnv.createTable(unifiedRevisionTableNameWithCatalog , icebergRevisionTableDescriptor)
    }

    val selectResult = revisionCreateStreamTable.select(
      $("database").as("wiki_id"),
      $("page_id"),
      $("rev_id"),
      $("comment")
    )

    val pipeline = selectResult.insertInto(unifiedRevisionTableNameWithCatalog)
    pipeline.printExplain()
    pipeline.execute()
  }
}
