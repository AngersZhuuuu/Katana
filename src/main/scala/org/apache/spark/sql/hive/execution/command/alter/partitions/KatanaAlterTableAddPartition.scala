package org.apache.spark.sql.hive.execution.command.alter.partitions

import org.apache.spark.sql.{Row, SparkSession}
import org.apache.spark.sql.catalyst.catalog.{CatalogStatistics, CatalogTablePartition, CatalogUtils}
import org.apache.spark.sql.execution.command.{AlterTableAddPartitionCommand, DDLUtils, RunnableCommand}
import org.apache.spark.sql.execution.datasources.PartitioningUtils
import org.apache.spark.sql.hive.{CatalogSchemaUtil, KatanaContext}
import org.apache.spark.sql.hive.execution.command.KatanaCommandUtils

/**
  * @author angers.zhu@gmail.com
  * @date 2019/5/30 17:27
  */
case class KatanaAlterTableAddPartition(delegate: AlterTableAddPartitionCommand)
                                       (@transient private val session: SparkSession,
                                        @transient private val katana: KatanaContext) extends RunnableCommand {

  override def run(sparkSession: SparkSession): Seq[Row] = {
    val catalog =
      CatalogSchemaUtil.getCatalog(
        delegate.tableName.catalog,
        sparkSession,
        katana)

    val table = catalog.getTableMetadata(delegate.tableName)
    DDLUtils.verifyAlterTableType(catalog, table, isView = false)
    DDLUtils.verifyPartitionProviderIsHive(sparkSession, table, "ALTER TABLE ADD PARTITION")
    val parts = delegate.partitionSpecsAndLocs.map { case (spec, location) =>
      val normalizedSpec = PartitioningUtils.normalizePartitionSpec(
        spec,
        table.partitionColumnNames,
        table.identifier.quotedString,
        session.sessionState.conf.resolver)
      // inherit table storage format (possibly except for location)
      CatalogTablePartition(normalizedSpec, table.storage.copy(
        locationUri = location.map(CatalogUtils.stringToURI)))
    }
    catalog.createPartitions(table.identifier, parts, ignoreIfExists = delegate.ifNotExists)

    if (table.stats.nonEmpty) {
      if (session.sessionState.conf.autoSizeUpdateEnabled) {
        val addedSize = parts.map { part =>
          KatanaCommandUtils.calculateLocationSize(session.sessionState, table.identifier,
            part.storage.locationUri)
        }.sum
        if (addedSize > 0) {
          val newStats = CatalogStatistics(sizeInBytes = table.stats.get.sizeInBytes + addedSize)
          catalog.alterTableStats(table.identifier, Some(newStats))
        }
      } else {
        catalog.alterTableStats(table.identifier, None)
      }
    }
    Seq.empty[Row]
  }
}
