package org.apache.spark.sql.hive.execution.command

import java.net.URI

import org.apache.hadoop.fs.{FileSystem, Path, PathFilter}
import org.apache.spark.internal.Logging
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.catalyst.TableIdentifier
import org.apache.spark.sql.catalyst.catalog.{CatalogStatistics, CatalogTable, SessionCatalog}
import org.apache.spark.sql.execution.datasources.{DataSourceUtils, InMemoryFileIndex}
import org.apache.spark.sql.internal.SessionState

import scala.util.control.NonFatal

/**
  * @author angers.zhu@gmail.com
  * @date 2019/5/28 19:54
  */
object KatanaCommandUtils extends Logging {

  /** Change statistics after changing data by commands. */
  def updateTableStats(catalog: SessionCatalog, sessionState: SessionState, sparkSession: SparkSession, table: CatalogTable): Unit = {
    if (table.stats.nonEmpty) {
      if (sessionState.conf.autoSizeUpdateEnabled) {
        val newTable = catalog.getTableMetadata(table.identifier)
        val newSize = KatanaCommandUtils.calculateTotalSize(catalog, table.identifier, sessionState, sparkSession, newTable)
        val newStats = CatalogStatistics(sizeInBytes = newSize)
        catalog.alterTableStats(table.identifier, Some(newStats))
      } else {
        catalog.alterTableStats(table.identifier, None)
      }
    }
  }

  def calculateTotalSize(catalog: SessionCatalog, tableIdentifierWithSchema: TableIdentifier, sessionState: SessionState, spark: SparkSession, catalogTable: CatalogTable): BigInt = {
    if (catalogTable.partitionColumnNames.isEmpty) {
      calculateLocationSize(sessionState, tableIdentifierWithSchema, catalogTable.storage.locationUri)
    } else {
      // Calculate table size as a sum of the visible partitions. See SPARK-21079
      val partitions = catalog.listPartitions(catalogTable.identifier)
      if (sessionState.conf.parallelFileListingInStatsComputation) {
        val paths = partitions.map(x => new Path(x.storage.locationUri.get))
        val stagingDir = sessionState.conf.getConfString("hive.exec.stagingdir", ".hive-staging")
        val pathFilter = new PathFilter with Serializable {
          override def accept(path: Path): Boolean = {
            DataSourceUtils.isDataPath(path) && !path.getName.startsWith(stagingDir)
          }
        }
        val fileStatusSeq = InMemoryFileIndex.bulkListLeafFiles(
          paths, sessionState.newHadoopConf(), pathFilter, spark)
        fileStatusSeq.flatMap(_._2.map(_.getLen)).sum
      } else {
        partitions.map { p =>
          calculateLocationSize(sessionState, tableIdentifierWithSchema, p.storage.locationUri)
        }.sum
      }
    }
  }

  def calculateLocationSize(sessionState: SessionState,
                            identifier: TableIdentifier,
                            locationUri: Option[URI]): Long = {
    // This method is mainly based on
    // org.apache.hadoop.hive.ql.stats.StatsUtils.getFileSizeForTable(HiveConf, Table)
    // in Hive 0.13 (except that we do not use fs.getContentSummary).
    // TODO: Generalize statistics collection.
    // TODO: Why fs.getContentSummary returns wrong size on Jenkins?
    // Can we use fs.getContentSummary in future?
    // Seems fs.getContentSummary returns wrong table size on Jenkins. So we use
    // countFileSize to count the table size.
    val stagingDir = sessionState.conf.getConfString("hive.exec.stagingdir", ".hive-staging")

    def getPathSize(fs: FileSystem, path: Path): Long = {
      val fileStatus = fs.getFileStatus(path)
      val size = if (fileStatus.isDirectory) {
        fs.listStatus(path)
          .map { status =>
            if (!status.getPath.getName.startsWith(stagingDir) &&
              DataSourceUtils.isDataPath(path)) {
              getPathSize(fs, status.getPath)
            } else {
              0L
            }
          }.sum
      } else {
        fileStatus.getLen
      }

      size
    }

    val startTime = System.nanoTime()
    logInfo(s"Starting to calculate the total file size under path $locationUri.")
    val size = locationUri.map { p =>
      val path = new Path(p)
      try {
        val fs = path.getFileSystem(sessionState.newHadoopConf())
        getPathSize(fs, path)
      } catch {
        case NonFatal(e) =>
          logWarning(
            s"Failed to get the size of table ${identifier.table} in the " +
              s"database ${identifier.database} because of ${e.toString}", e)
          0L
      }
    }.getOrElse(0L)
    val durationInMs = (System.nanoTime() - startTime) / (1000 * 1000)
    logInfo(s"It took $durationInMs ms to calculate the total file size under path $locationUri.")

    size
  }

  def compareAndGetNewStats(oldStats: Option[CatalogStatistics],
                            newTotalSize: BigInt,
                            newRowCount: Option[BigInt]): Option[CatalogStatistics] = {
    val oldTotalSize = oldStats.map(_.sizeInBytes).getOrElse(BigInt(-1))
    val oldRowCount = oldStats.flatMap(_.rowCount).getOrElse(BigInt(-1))
    var newStats: Option[CatalogStatistics] = None
    if (newTotalSize >= 0 && newTotalSize != oldTotalSize) {
      newStats = Some(CatalogStatistics(sizeInBytes = newTotalSize))
    }
    // We only set rowCount when noscan is false, because otherwise:
    // 1. when total size is not changed, we don't need to alter the table;
    // 2. when total size is changed, `oldRowCount` becomes invalid.
    // This is to make sure that we only record the right statistics.
    if (newRowCount.isDefined) {
      if (newRowCount.get >= 0 && newRowCount.get != oldRowCount) {
        newStats = if (newStats.isDefined) {
          newStats.map(_.copy(rowCount = newRowCount))
        } else {
          Some(CatalogStatistics(sizeInBytes = oldTotalSize, rowCount = newRowCount))
        }
      }
    }
    newStats
  }
}

