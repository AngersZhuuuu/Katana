package org.apache.spark.sql.hive.execution.command.alter.table

import org.apache.spark.sql.catalyst.TableIdentifier
import org.apache.spark.sql.catalyst.catalog.{CatalogUtils, SessionCatalog}
import org.apache.spark.sql.execution.command._
import org.apache.spark.sql.hive.execution.command.KatanaCommandUtils
import org.apache.spark.sql.hive.{KatanaContext, CatalogSchemaUtil}
import org.apache.spark.sql.internal.SessionState
import org.apache.spark.sql.{Row, SparkSession}

import scala.collection.mutable.HashMap

/**
  * @author angers.zhu@gmail.com
  * @date 2019/5/30 17:08
  */
case class KatanaAlterTableSetLocation(delegate: AlterTableSetLocationCommand,
                                       hiveCatalogs: HashMap[String, SessionCatalog])
                                      (@transient private val sessionState: SessionState,
                                       @transient private val katana: KatanaContext) extends RunnableCommand {

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

    val originOldTableIdentifier = new TableIdentifier(delegate.tableName.table, Some(originDB))

    val table = catalog.getTableMetadata(originOldTableIdentifier)
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

    KatanaCommandUtils.updateTableStats(catalog, sessionState, sparkSession, table)
    Seq.empty[Row]
  }
}
