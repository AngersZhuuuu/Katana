package org.apache.spark.sql.hive.execution.command.alter.partitions

import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.{FileStatus, FileSystem, Path, PathFilter}
import org.apache.hadoop.mapred.{FileInputFormat, JobConf}
import org.apache.spark.sql.catalyst.TableIdentifier
import org.apache.spark.sql.catalyst.analysis.Resolver
import org.apache.spark.sql.catalyst.catalog.CatalogTypes.TablePartitionSpec
import org.apache.spark.sql.catalyst.catalog.{CatalogTable, CatalogTablePartition, ExternalCatalogUtils, SessionCatalog}
import org.apache.spark.sql.execution.command.{AlterTableRecoverPartitionsCommand, DDLUtils, PartitionStatistics, RunnableCommand}
import org.apache.spark.sql.hive.{KatanaContext, CatalogSchemaUtil}
import org.apache.spark.sql.internal.SessionState
import org.apache.spark.sql.{AnalysisException, Row, SparkSession}
import org.apache.spark.util.{SerializableConfiguration, ThreadUtils}

import scala.collection.mutable.HashMap
import scala.collection.parallel.ForkJoinTaskSupport
import scala.collection.{GenMap, GenSeq}

/**
  * @author angers.zhu@gmail.com
  * @date 2019/5/30 18:21
  */
case class KatanaAlterTableRecoverPartitions(delegate: AlterTableRecoverPartitionsCommand,
                                             hiveCatalogs: HashMap[String, SessionCatalog])
                                            (@transient private val sessionState: SessionState,
                                             @transient private val katana: KatanaContext) extends RunnableCommand {

  // These are list of statistics that can be collected quickly without requiring a scan of the data
  // see https://github.com/apache/hive/blob/master/
  //   common/src/java/org/apache/hadoop/hive/common/StatsSetupConst.java
  val NUM_FILES = "numFiles"
  val TOTAL_SIZE = "totalSize"
  val DDL_TIME = "transient_lastDdlTime"

  private def getPathFilter(hadoopConf: Configuration): PathFilter = {
    // Dummy jobconf to get to the pathFilter defined in configuration
    // It's very expensive to create a JobConf(ClassUtil.findContainingJar() is slow)
    val jobConf = new JobConf(hadoopConf, this.getClass)
    val pathFilter = FileInputFormat.getInputPathFilter(jobConf)
    new PathFilter {
      override def accept(path: Path): Boolean = {
        val name = path.getName
        if (name != "_SUCCESS" && name != "_temporary" && !name.startsWith(".")) {
          pathFilter == null || pathFilter.accept(path)
        } else {
          false
        }
      }
    }
  }

  override def run(sparkSession: SparkSession): Seq[Row] = {
    val catalog =
      CatalogSchemaUtil.getCatalog(
        delegate.tableName.catalog,
        hiveCatalogs,
        sparkSession,
        katana)

    val table = catalog.getTableMetadata(delegate.tableName)
    val tableIdentWithDB = table.identifier.quotedString
    DDLUtils.verifyAlterTableType(catalog, table, isView = false)
    if (table.partitionColumnNames.isEmpty) {
      throw new AnalysisException(
        s"Operation not allowed: ${delegate.cmd} only works on partitioned tables: $tableIdentWithDB")
    }

    if (table.storage.locationUri.isEmpty) {
      throw new AnalysisException(s"Operation not allowed: ${delegate.cmd} only works on table with " +
        s"location provided: $tableIdentWithDB")
    }

    val root = new Path(table.location)
    logInfo(s"Recover all the partitions in $root")
    val hadoopConf = sparkSession.sessionState.newHadoopConf()
    val fs = root.getFileSystem(hadoopConf)

    val threshold = sparkSession.conf.get("spark.rdd.parallelListingThreshold", "10").toInt
    val pathFilter = getPathFilter(hadoopConf)

    val evalPool = ThreadUtils.newForkJoinPool("AlterTableRecoverPartitionsCommand", 8)
    val partitionSpecsAndLocs: Seq[(TablePartitionSpec, Path)] =
      try {
        scanPartitions(fs, pathFilter, root, Map(), table.partitionColumnNames, threshold,
          sparkSession.sessionState.conf.resolver, new ForkJoinTaskSupport(evalPool)).seq
      } finally {
        evalPool.shutdown()
      }
    val total = partitionSpecsAndLocs.length
    logInfo(s"Found $total partitions in $root")

    val partitionStats = if (sparkSession.sqlContext.conf.gatherFastStats) {
      gatherPartitionStats(sparkSession, partitionSpecsAndLocs, fs, pathFilter, threshold)
    } else {
      GenMap.empty[String, PartitionStatistics]
    }
    logInfo(s"Finished to gather the fast stats for all $total partitions.")

    addPartitions(catalog, delegate.tableName, table, partitionSpecsAndLocs, partitionStats)
    // Updates the table to indicate that its partition metadata is stored in the Hive metastore.
    // This is always the case for Hive format tables, but is not true for Datasource tables created
    // before Spark 2.1 unless they are converted via `msck repair table`.
    catalog.alterTable(table.copy(tracksPartitionsInCatalog = true))
    catalog.refreshTable(delegate.tableName)
    logInfo(s"Recovered all partitions ($total).")
    Seq.empty[Row]
  }

  private def scanPartitions(fs: FileSystem,
                             filter: PathFilter,
                             path: Path,
                             spec: TablePartitionSpec,
                             partitionNames: Seq[String],
                             threshold: Int,
                             resolver: Resolver,
                             evalTaskSupport: ForkJoinTaskSupport): GenSeq[(TablePartitionSpec, Path)] = {
    if (partitionNames.isEmpty) {
      return Seq(spec -> path)
    }

    val statuses = fs.listStatus(path, filter)
    val statusPar: GenSeq[FileStatus] =
      if (partitionNames.length > 1 && statuses.length > threshold || partitionNames.length > 2) {
        // parallelize the list of partitions here, then we can have better parallelism later.
        val parArray = statuses.par
        parArray.tasksupport = evalTaskSupport
        parArray
      } else {
        statuses
      }
    statusPar.flatMap { st =>
      val name = st.getPath.getName
      if (st.isDirectory && name.contains("=")) {
        val ps = name.split("=", 2)
        val columnName = ExternalCatalogUtils.unescapePathName(ps(0))
        // TODO: Validate the value
        val value = ExternalCatalogUtils.unescapePathName(ps(1))
        if (resolver(columnName, partitionNames.head)) {
          scanPartitions(fs, filter, st.getPath, spec ++ Map(partitionNames.head -> value),
            partitionNames.drop(1), threshold, resolver, evalTaskSupport)
        } else {
          logWarning(
            s"expected partition column ${partitionNames.head}, but got ${ps(0)}, ignoring it")
          Seq.empty
        }
      } else {
        logWarning(s"ignore ${new Path(path, name)}")
        Seq.empty
      }
    }
  }

  private def gatherPartitionStats(
                                    spark: SparkSession,
                                    partitionSpecsAndLocs: GenSeq[(TablePartitionSpec, Path)],
                                    fs: FileSystem,
                                    pathFilter: PathFilter,
                                    threshold: Int): GenMap[String, PartitionStatistics] = {
    if (partitionSpecsAndLocs.length > threshold) {
      val hadoopConf = spark.sessionState.newHadoopConf()
      val serializableConfiguration = new SerializableConfiguration(hadoopConf)
      val serializedPaths = partitionSpecsAndLocs.map(_._2.toString).toArray

      // Set the number of parallelism to prevent following file listing from generating many tasks
      // in case of large #defaultParallelism.
      val numParallelism = Math.min(serializedPaths.length,
        Math.min(spark.sparkContext.defaultParallelism, 10000))
      // gather the fast stats for all the partitions otherwise Hive metastore will list all the
      // files for all the new partitions in sequential way, which is super slow.
      logInfo(s"Gather the fast stats in parallel using $numParallelism tasks.")
      spark.sparkContext.parallelize(serializedPaths, numParallelism)
        .mapPartitions { paths =>
          val pathFilter = getPathFilter(serializableConfiguration.value)
          paths.map(new Path(_)).map { path =>
            val fs = path.getFileSystem(serializableConfiguration.value)
            val statuses = fs.listStatus(path, pathFilter)
            (path.toString, PartitionStatistics(statuses.length, statuses.map(_.getLen).sum))
          }
        }.collectAsMap()
    } else {
      partitionSpecsAndLocs.map { case (_, location) =>
        val statuses = fs.listStatus(location, pathFilter)
        (location.toString, PartitionStatistics(statuses.length, statuses.map(_.getLen).sum))
      }.toMap
    }
  }

  private def addPartitions(catalog: SessionCatalog,
                            tableName: TableIdentifier,
                            table: CatalogTable,
                            partitionSpecsAndLocs: GenSeq[(TablePartitionSpec, Path)],
                            partitionStats: GenMap[String, PartitionStatistics]): Unit = {
    val total = partitionSpecsAndLocs.length
    var done = 0L
    // Hive metastore may not have enough memory to handle millions of partitions in single RPC,
    // we should split them into smaller batches. Since Hive client is not thread safe, we cannot
    // do this in parallel.
    val batchSize = 100
    partitionSpecsAndLocs.toIterator.grouped(batchSize).foreach { batch =>
      val now = System.currentTimeMillis() / 1000
      val parts = batch.map { case (spec, location) =>
        val params = partitionStats.get(location.toString).map {
          case PartitionStatistics(numFiles, totalSize) =>
            // This two fast stat could prevent Hive metastore to list the files again.
            Map(NUM_FILES -> numFiles.toString,
              TOTAL_SIZE -> totalSize.toString,
              // Workaround a bug in HiveMetastore that try to mutate a read-only parameters.
              // see metastore/src/java/org/apache/hadoop/hive/metastore/HiveMetaStore.java
              DDL_TIME -> now.toString)
        }.getOrElse(Map.empty)
        // inherit table storage format (possibly except for location)
        CatalogTablePartition(
          spec,
          table.storage.copy(locationUri = Some(location.toUri)),
          params)
      }
      catalog.createPartitions(tableName, parts, ignoreIfExists = true)
      done += parts.length
      logDebug(s"Recovered ${parts.length} partitions ($done/$total so far)")
    }
  }
}
