package org.apache.spark.sql.hive.execution.command.alter.table

import org.apache.spark.sql.{Row, SparkSession}
import org.apache.spark.sql.catalyst.catalog.CatalogUtils
import org.apache.spark.sql.execution.command._
import org.apache.spark.sql.hive.{CatalogSchemaUtil, KatanaContext}
import org.apache.spark.sql.hive.execution.command.KatanaCommandUtils

/**
  * @author angers.zhu@gmail.com
  * @date 2019/5/30 17:08
  */
case class KatanaAlterTableSetLocation(delegate: AlterTableSetLocationCommand)
                                      (@transient private val katana: KatanaContext) extends RunnableCommand {

  override def run(sparkSession: SparkSession): Seq[Row] = {
    val session = CatalogSchemaUtil.getSession(delegate.tableName.catalog, sparkSession, katana)
    val catalog = session.sessionState.catalog

    val table = catalog.getTableMetadata(delegate.tableName)
    val locUri = CatalogUtils.stringToURI(delegate.location)
    DDLUtils.verifyAlterTableType(catalog, table, isView = false)
    delegate.partitionSpec match {
      case Some(spec) =>
        DDLUtils.verifyPartitionProviderIsHive(
          sparkSession, table, "ALTER TABLE ... SET LOCATION")
        // Partition spec is specified, so we set the location only for this partition
        val part = catalog.getPartition(table.identifier, spec)
        val newPart = part.copy(storage = part.storage.copy(locationUri = Some(locUri)))
        catalog.alterPartitions(table.identifier, Seq(newPart))
      case None =>
        // No partition spec is specified, so we set the location for the table itself
        catalog.alterTable(table.withNewStorage(locationUri = Some(locUri)))
    }

    KatanaCommandUtils.updateTableStats(catalog, session, sparkSession, table)
    Seq.empty[Row]
  }
}
