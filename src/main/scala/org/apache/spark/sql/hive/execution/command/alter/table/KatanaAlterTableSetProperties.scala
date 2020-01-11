package org.apache.spark.sql.hive.execution.command.alter.table

import org.apache.spark.sql.catalyst.TableIdentifier
import org.apache.spark.sql.{Row, SparkSession}
import org.apache.spark.sql.catalyst.catalog.SessionCatalog
import org.apache.spark.sql.execution.command.{AlterTableSetPropertiesCommand, DDLUtils, RunnableCommand}
import org.apache.spark.sql.hive.{KatanaContext, CatalogSchemaUtil}

import scala.collection.mutable.HashMap

/**
  * @author angers.zhu@gmail.com
  * @date 2019/5/30 17:17
  */
case class KatanaAlterTableSetProperties(delegate: AlterTableSetPropertiesCommand,
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

    val originOldTableIdentifier = new TableIdentifier(delegate.tableName.table, Some(originDB))

    val table = catalog.getTableMetadata(originOldTableIdentifier)
    DDLUtils.verifyAlterTableType(catalog, table, delegate.isView)
    // This overrides old properties and update the comment parameter of CatalogTable
    // with the newly added/modified comment since CatalogTable also holds comment as its
    // direct property.
    val newTable = table.copy(
      properties = table.properties ++ delegate.properties,
      comment = delegate.properties.get("comment").orElse(table.comment))
    catalog.alterTable(newTable)
    Seq.empty[Row]
  }
}