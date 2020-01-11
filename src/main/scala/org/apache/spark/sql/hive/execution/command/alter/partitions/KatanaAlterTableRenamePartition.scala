package org.apache.spark.sql.hive.execution.command.alter.partitions

import org.apache.spark.sql.catalyst.TableIdentifier
import org.apache.spark.sql.catalyst.catalog.SessionCatalog
import org.apache.spark.sql.execution.command.{AlterTableRenamePartitionCommand, DDLUtils, RunnableCommand}
import org.apache.spark.sql.execution.datasources.PartitioningUtils
import org.apache.spark.sql.hive.{KatanaContext, CatalogSchemaUtil}
import org.apache.spark.sql.{Row, SparkSession}

import scala.collection.mutable.HashMap

/**
  * @author angers.zhu@gmail.com
  * @date 2019/5/30 18:23
  */
case class KatanaAlterTableRenamePartition(delegate: AlterTableRenamePartitionCommand,
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

    val originTableIdentifier = new TableIdentifier(delegate.tableName.table, Some(originDB))
    val table = catalog.getTableMetadata(originTableIdentifier)
    DDLUtils.verifyAlterTableType(catalog, table, isView = false)
    DDLUtils.verifyPartitionProviderIsHive(sparkSession, table, "ALTER TABLE RENAME PARTITION")

    val normalizedOldPartition = PartitioningUtils.normalizePartitionSpec(
      delegate.oldPartition,
      table.partitionColumnNames,
      table.identifier.quotedString,
      sparkSession.sessionState.conf.resolver)

    val normalizedNewPartition = PartitioningUtils.normalizePartitionSpec(
      delegate.newPartition,
      table.partitionColumnNames,
      table.identifier.quotedString,
      sparkSession.sessionState.conf.resolver)

    catalog.renamePartitions(
      originTableIdentifier, Seq(normalizedOldPartition), Seq(normalizedNewPartition))
    Seq.empty[Row]
  }

}
