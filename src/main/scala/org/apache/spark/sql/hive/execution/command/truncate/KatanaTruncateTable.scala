package org.apache.spark.sql.hive.execution.command.truncate

import org.apache.hadoop.fs.Path
import org.apache.spark.sql.catalyst.TableIdentifier
import org.apache.spark.sql.catalyst.analysis.NoSuchPartitionException
import org.apache.spark.sql.{AnalysisException, Row, SparkSession}
import org.apache.spark.sql.catalyst.catalog.{CatalogStatistics, CatalogTableType, SessionCatalog}
import org.apache.spark.sql.execution.command.{DDLUtils, RunnableCommand, TruncateTableCommand}
import org.apache.spark.sql.execution.datasources.PartitioningUtils
import org.apache.spark.sql.hive.{KatanaContext, KatanaExtension, CatalogSchemaUtil}

import scala.collection.mutable.HashMap
import scala.util.control.NonFatal

/**
  * @author angers.zhu@gmail.com
  * @date 2019/5/30 10:11
  */
case class KatanaTruncateTable(delegate: TruncateTableCommand,
                               hiveCatalogs: HashMap[String, SessionCatalog])
                              (@transient private val katana: KatanaContext) extends RunnableCommand {

  override def run(sparkSession: SparkSession): Seq[Row] = {

    val (catalog: SessionCatalog, originDB: String) = delegate.tableName.database match {
      case None => {
        val tempCatalog =
          if (katana.getActiveSessionState() == null)
            sparkSession.sessionState.catalog
          else
            katana.getActiveSessionState().catalog
        (tempCatalog, tempCatalog.getCurrentDatabase)
      }
      case Some(db) => CatalogSchemaUtil.getCatalogAndOriginDBName(hiveCatalogs, db)(sparkSession)
    }


    /**
      * 原生Identifier用于本Identifier所属的Catalog 进行查询
      */
    val originTableIdentifier = new TableIdentifier(delegate.tableName.table, Some(originDB))

    /**
      * 带有Schema的DB 用于在SparkSession 生成 UnresolvedRelation 进行路由
      */
    val hiveSchema = hiveCatalogs.find(_._2 == catalog)
    val withSchemaDB = if (hiveSchema.isDefined) hiveSchema.get._1 + "_" + originDB else originDB
    val tableIdentifierWithSchema = new TableIdentifier(delegate.tableName.table, Some(withSchemaDB))

    val table = catalog.getTableMetadata(originTableIdentifier)


    if (table.tableType == CatalogTableType.EXTERNAL) {
      throw new AnalysisException(
        s"Operation not allowed: TRUNCATE TABLE on external tables: $tableIdentifierWithSchema")
    }
    if (table.tableType == CatalogTableType.VIEW) {
      throw new AnalysisException(
        s"Operation not allowed: TRUNCATE TABLE on views: $tableIdentifierWithSchema")
    }
    if (table.partitionColumnNames.isEmpty && delegate.partitionSpec.isDefined) {
      throw new AnalysisException(
        s"Operation not allowed: TRUNCATE TABLE ... PARTITION is not supported " +
          s"for tables that are not partitioned: $tableIdentifierWithSchema")
    }
    if (delegate.partitionSpec.isDefined) {
      DDLUtils.verifyPartitionProviderIsHive(sparkSession, table, "TRUNCATE TABLE ... PARTITION")
    }

    val partCols = table.partitionColumnNames
    val locations =
      if (partCols.isEmpty) {
        Seq(table.storage.locationUri)
      } else {
        val normalizedSpec = delegate.partitionSpec.map { spec =>
          PartitioningUtils.normalizePartitionSpec(
            spec,
            partCols,
            table.identifier.quotedString,
            sparkSession.sessionState.conf.resolver)
        }
        val partLocations =
          catalog.listPartitions(table.identifier, normalizedSpec).map(_.storage.locationUri)

        // Fail if the partition spec is fully specified (not partial) and the partition does not
        // exist.
        for (spec <- delegate.partitionSpec if partLocations.isEmpty && spec.size == partCols.length) {
          throw new NoSuchPartitionException(table.database, table.identifier.table, spec)
        }

        partLocations
      }
    val hadoopConf = sparkSession.sessionState.newHadoopConf()
    locations.foreach { location =>
      if (location.isDefined) {
        val path = new Path(location.get)
        try {
          val fs = path.getFileSystem(hadoopConf)
          fs.delete(path, true)
          fs.mkdirs(path)
        } catch {
          case NonFatal(e) =>
            throw new AnalysisException(
              s"Failed to truncate table $tableIdentifierWithSchema when removing data of the path: $path " +
                s"because of ${e.toString}")
        }
      }
    }
    // After deleting the data, invalidate the table to make sure we don't keep around a stale
    // file relation in the metastore cache.
    catalog.refreshTable(originTableIdentifier)
    // Also try to drop the contents of the table from the columnar cache
    try {
      sparkSession.sharedState.cacheManager.uncacheQuery(sparkSession.table(tableIdentifierWithSchema), cascade = true)
    } catch {
      case NonFatal(e) =>
        log.warn(s"Exception when attempting to uncache table $tableIdentifierWithSchema", e)
    }

    if (table.stats.nonEmpty) {
      // empty table after truncation
      val newStats = CatalogStatistics(sizeInBytes = 0, rowCount = Some(0))
      catalog.alterTableStats(originTableIdentifier, Some(newStats))
    }
    Seq.empty[Row]
  }
}
