package org.apache.spark.sql.hive.execution

import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.Path
import org.apache.hadoop.hive.ql.ErrorMsg
import org.apache.hadoop.hive.ql.plan.TableDesc
import org.apache.spark.SparkException
import org.apache.spark.sql.{AnalysisException, Row, SparkSession}
import org.apache.spark.sql.catalyst.catalog._
import org.apache.spark.sql.catalyst.expressions.Attribute
import org.apache.spark.sql.catalyst.plans.logical.LogicalPlan
import org.apache.spark.sql.execution.SparkPlan
import org.apache.spark.sql.hive.HiveShim.{ShimFileSinkDesc => FileSinkDesc}
import org.apache.spark.sql.hive.client.HiveClientImpl
import org.apache.spark.sql.hive.execution.command.KatanaCommandUtils
import org.apache.spark.sql.internal.SessionState

/**
 * @author angers.zhu@gmail.com
 * @date 2019/5/28 19:14
 *
 *       因为原油的InsertIntoHiveTable使用的是默认的SparkSession的session catalog
 *       在这里使用 KatanaInsertIntoHiveTable 做替换
 */
case class KatanaInsertIntoHiveTable(table: CatalogTable,
                                     partition: Map[String, Option[String]],
                                     query: LogicalPlan,
                                     overwrite: Boolean,
                                     ifPartitionNotExists: Boolean,
                                     outputColumnNames: Seq[String])
                                    (@transient private val catalog: SessionCatalog,
                                     @transient private val sessionState: SessionState) extends SaveAsHiveFile {

  /**
   * Inserts all the rows in the table into Hive.  Row objects are properly serialized with the
   * `org.apache.hadoop.hive.serde2.SerDe` and the
   * `org.apache.hadoop.mapred.OutputFormat` provided by the table definition.
   */
  override def run(sparkSession: SparkSession, child: SparkPlan): Seq[Row] = {
    val externalCatalog = catalog.externalCatalog
    val hadoopConf = sessionState.newHadoopConf()

    val hiveQlTable = HiveClientImpl.toHiveTable(table)
    // Have to pass the TableDesc object to RDD.mapPartitions and then instantiate new serializer
    // instances within the closure, since Serializer is not serializable while TableDesc is.
    val tableDesc = new TableDesc(
      hiveQlTable.getInputFormatClass,
      // The class of table should be org.apache.hadoop.hive.ql.metadata.Table because
      // getOutputFormatClass will use HiveFileFormatUtils.getOutputFormatSubstitute to
      // substitute some output formats, e.g. substituting SequenceFileOutputFormat to
      // HiveSequenceFileOutputFormat.
      hiveQlTable.getOutputFormatClass,
      hiveQlTable.getMetadata
    )
    val tableLocation = hiveQlTable.getDataLocation
    val tmpLocation = getExternalTmpPath(sparkSession, hadoopConf, tableLocation)
    try {
      processInsert(sparkSession, externalCatalog, hadoopConf, tableDesc, tmpLocation, child)
    } finally {
      // Attempt to delete the staging directory and the inclusive files. If failed, the files are
      // expected to be dropped at the normal termination of VM since deleteOnExit is used.
      deleteExternalTmpPath(hadoopConf)
    }

    /**
     * 如果是额外配置的Hive，则其表没有在SparkSession 的catalog缓存中，
     * 也没必要un-cache，且会报错
     */
    if (catalog == sparkSession.sessionState.catalog) {
      sparkSession.catalog.uncacheTable(table.identifier.quotedString)
    }
    catalog.refreshTable(table.identifier)
    KatanaCommandUtils.updateTableStats(catalog, sessionState, sparkSession, table)

    // It would be nice to just return the childRdd unchanged so insert operations could be chained,
    // however for now we return an empty list to simplify compatibility checks with hive, which
    // does not return anything for insert operations.
    // TODO: implement hive compatibility as rules.
    Seq.empty[Row]
  }

  private def processInsert(sparkSession: SparkSession,
                            externalCatalog: ExternalCatalog,
                            hadoopConf: Configuration,
                            tableDesc: TableDesc,
                            tmpLocation: Path,
                            child: SparkPlan): Unit = {
    val fileSinkConf = new FileSinkDesc(tmpLocation.toString, tableDesc, false)

    val numDynamicPartitions = partition.values.count(_.isEmpty)
    val numStaticPartitions = partition.values.count(_.nonEmpty)
    val partitionSpec = partition.map {
      case (key, Some(value)) => key -> value
      case (key, None) => key -> ""
    }

    // All partition column names in the format of "<column name 1>/<column name 2>/..."
    val partitionColumns = fileSinkConf.getTableInfo.getProperties.getProperty("partition_columns")
    val partitionColumnNames = Option(partitionColumns).map(_.split("/")).getOrElse(Array.empty)

    // By this time, the partition map must match the table's partition columns
    if (partitionColumnNames.toSet != partition.keySet) {
      throw new SparkException(
        s"""Requested partitioning does not match the ${table.identifier.table} table:
           |Requested partitions: ${partition.keys.mkString(",")}
           |Table partitions: ${table.partitionColumnNames.mkString(",")}""".stripMargin)
    }

    // Validate partition spec if there exist any dynamic partitions
    if (numDynamicPartitions > 0) {
      // Report error if dynamic partitioning is not enabled
      if (!hadoopConf.get("hive.exec.dynamic.partition", "true").toBoolean) {
        throw new SparkException(ErrorMsg.DYNAMIC_PARTITION_DISABLED.getMsg)
      }

      // Report error if dynamic partition strict mode is on but no static partition is found
      if (numStaticPartitions == 0 &&
        hadoopConf.get("hive.exec.dynamic.partition.mode", "strict").equalsIgnoreCase("strict")) {
        throw new SparkException(ErrorMsg.DYNAMIC_PARTITION_STRICT_MODE.getMsg)
      }

      // Report error if any static partition appears after a dynamic partition
      val isDynamic = partitionColumnNames.map(partitionSpec(_).isEmpty)
      if (isDynamic.init.zip(isDynamic.tail).contains((true, false))) {
        throw new AnalysisException(ErrorMsg.PARTITION_DYN_STA_ORDER.getMsg)
      }
    }

    table.bucketSpec match {
      case Some(bucketSpec) =>
        // Writes to bucketed hive tables are allowed only if user does not care about maintaining
        // table's bucketing ie. both "hive.enforce.bucketing" and "hive.enforce.sorting" are
        // set to false
        val enforceBucketingConfig = "hive.enforce.bucketing"
        val enforceSortingConfig = "hive.enforce.sorting"

        val message = s"Output Hive table ${table.identifier} is bucketed but Spark " +
          "currently does NOT populate bucketed output which is compatible with Hive."

        if (hadoopConf.get(enforceBucketingConfig, "true").toBoolean ||
          hadoopConf.get(enforceSortingConfig, "true").toBoolean) {
          throw new AnalysisException(message)
        } else {
          logWarning(message + s" Inserting data anyways since both $enforceBucketingConfig and " +
            s"$enforceSortingConfig are set to false.")
        }
      case _ => // do nothing since table has no bucketing
    }

    val partitionAttributes = partitionColumnNames.takeRight(numDynamicPartitions).map { name =>
      query.resolve(name :: Nil, sparkSession.sessionState.analyzer.resolver).getOrElse {
        throw new AnalysisException(
          s"Unable to resolve $name given [${query.output.map(_.name).mkString(", ")}]")
      }.asInstanceOf[Attribute]
    }

    val writtenParts = saveAsHiveFile(
      sparkSession = sparkSession,
      plan = child,
      hadoopConf = hadoopConf,
      fileSinkConf = fileSinkConf,
      outputLocation = tmpLocation.toString,
      partitionAttributes = partitionAttributes)

    if (partition.nonEmpty) {
      if (numDynamicPartitions > 0) {
        if (overwrite && table.tableType == CatalogTableType.EXTERNAL) {
          // SPARK-29295: When insert overwrite to a Hive external table partition, if the
          // partition does not exist, Hive will not check if the external partition directory
          // exists or not before copying files. So if users drop the partition, and then do
          // insert overwrite to the same partition, the partition will have both old and new
          // data. We construct partition path. If the path exists, we delete it manually.
          writtenParts.foreach { partPath =>
            val dpMap = partPath.split("/").map { part =>
              val splitPart = part.split("=")
              assert(splitPart.size == 2, s"Invalid written partition path: $part")
              ExternalCatalogUtils.unescapePathName(splitPart(0)) ->
                ExternalCatalogUtils.unescapePathName(splitPart(1))
            }.toMap

            val updatedPartitionSpec = partition.map {
              case (key, Some(value)) => key -> value
              case (key, None) if dpMap.contains(key) => key -> dpMap(key)
              case (key, _) =>
                throw new SparkException(s"Dynamic partition key $key is not among " +
                  "written partition paths.")
            }
            val partitionColumnNames = table.partitionColumnNames
            val tablePath = new Path(table.location)
            val partitionPath = ExternalCatalogUtils.generatePartitionPath(updatedPartitionSpec,
              partitionColumnNames, tablePath)

            val fs = partitionPath.getFileSystem(hadoopConf)
            if (fs.exists(partitionPath)) {
              if (!fs.delete(partitionPath, true)) {
                throw new RuntimeException(
                  "Cannot remove partition directory '" + partitionPath.toString)
              }
            }
          }
        }

        externalCatalog.loadDynamicPartitions(
          db = table.database,
          table = table.identifier.table,
          tmpLocation.toString,
          partitionSpec,
          overwrite,
          numDynamicPartitions)
      } else {
        // scalastyle:off
        // ifNotExists is only valid with static partition, refer to
        // https://cwiki.apache.org/confluence/display/Hive/LanguageManual+DML#LanguageManualDML-InsertingdataintoHiveTablesfromqueries
        // scalastyle:on
        val oldPart =
        externalCatalog.getPartitionOption(
          table.database,
          table.identifier.table,
          partitionSpec)

        var doHiveOverwrite = overwrite

        if (oldPart.isEmpty || !ifPartitionNotExists) {
          // SPARK-29295: When insert overwrite to a Hive external table partition, if the
          // partition does not exist, Hive will not check if the external partition directory
          // exists or not before copying files. So if users drop the partition, and then do
          // insert overwrite to the same partition, the partition will have both old and new
          // data. We construct partition path. If the path exists, we delete it manually.
          val partitionPath = if (oldPart.isEmpty && overwrite
            && table.tableType == CatalogTableType.EXTERNAL) {
            val partitionColumnNames = table.partitionColumnNames
            val tablePath = new Path(table.location)
            Some(ExternalCatalogUtils.generatePartitionPath(partitionSpec,
              partitionColumnNames, tablePath))
          } else {
            oldPart.flatMap(_.storage.locationUri.map(uri => new Path(uri)))
          }
          // SPARK-18107: Insert overwrite runs much slower than hive-client.
          // Newer Hive largely improves insert overwrite performance. As Spark uses older Hive
          // version and we may not want to catch up new Hive version every time. We delete the
          // Hive partition first and then load data file into the Hive partition.
          if (partitionPath.nonEmpty && overwrite) {
            partitionPath.foreach { path =>
              val fs = path.getFileSystem(hadoopConf)
              if (fs.exists(path)) {
                if (!fs.delete(path, true)) {
                  throw new RuntimeException(
                    "Cannot remove partition directory '" + path.toString)
                }
                // Don't let Hive do overwrite operation since it is slower.
                doHiveOverwrite = false
              }
            }
          }

          // inheritTableSpecs is set to true. It should be set to false for an IMPORT query
          // which is currently considered as a Hive native command.
          val inheritTableSpecs = true
          externalCatalog.loadPartition(
            table.database,
            table.identifier.table,
            tmpLocation.toString,
            partitionSpec,
            isOverwrite = doHiveOverwrite,
            inheritTableSpecs = inheritTableSpecs,
            isSrcLocal = false)
        }
      }
    } else {
      externalCatalog.loadTable(
        table.database,
        table.identifier.table,
        tmpLocation.toString, // TODO: URI
        overwrite,
        isSrcLocal = false)
    }
  }
}
