package org.apache.spark.sql.hive.execution.command.alter.table

import org.apache.spark.sql.catalyst.TableIdentifier
import org.apache.spark.sql.{Row, SparkSession}
import org.apache.spark.sql.catalyst.catalog.SessionCatalog
import org.apache.spark.sql.execution.command.{AlterTableRenameCommand, DDLUtils, RunnableCommand}
import org.apache.spark.sql.hive.{KatanaContext, CatalogSchemaUtil}

import scala.collection.mutable.HashMap
import scala.util.Try
import scala.util.control.NonFatal

/**
  * @author angers.zhu@gmail.com
  * @date 2019/5/30 16:57
  */
case class KatanaAlterTableRename(delegate: AlterTableRenameCommand,
                                  hiveCatalogs: HashMap[String, SessionCatalog])
                                 (@transient private val katana: KatanaContext)extends RunnableCommand {

  override def run(sparkSession: SparkSession): Seq[Row] = {

    val (catalog: SessionCatalog, originDB: String) = delegate.oldName.database match {
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


    val hiveSchema = hiveCatalogs.find(_._2 == catalog)
    val withSchemaDB = if (hiveSchema.isDefined) hiveSchema.get._1 + "_" + originDB else originDB

    val originOldTableIdentifierWithSchema = new TableIdentifier(delegate.oldName.table, Some(withSchemaDB))
    val originOldTableIdentifier = new TableIdentifier(delegate.oldName.table, Some(originDB))

    val originNewTableIdentifierWithSchema = new TableIdentifier(delegate.newName.table, Some(withSchemaDB))
    val originNewTableIdentifierWithOutSchema = new TableIdentifier(delegate.newName.table, Some(originDB))

    // If this is a temp view, just rename the view.
    // Otherwise, if this is a real table, we also need to uncache and invalidate the table.
    if (catalog.isTemporaryTable(originOldTableIdentifier)) {
      catalog.renameTable(originOldTableIdentifier, originNewTableIdentifierWithOutSchema)
    } else {
      val table = catalog.getTableMetadata(originOldTableIdentifier)
      DDLUtils.verifyAlterTableType(catalog, table, delegate.isView)
      // If an exception is thrown here we can just assume the table is uncached;
      // this can happen with Hive tables when the underlying catalog is in-memory.
      val wasCached = Try(sparkSession.catalog.isCached(originOldTableIdentifierWithSchema.unquotedString)).getOrElse(false)
      if (wasCached) {
        try {
          sparkSession.catalog.uncacheTable(originOldTableIdentifierWithSchema.unquotedString)
        } catch {
          case NonFatal(e) => log.warn(e.toString, e)
        }
      }
      // Invalidate the table last, otherwise uncaching the table would load the logical plan
      // back into the hive metastore cache
      catalog.refreshTable(originOldTableIdentifier)
      catalog.renameTable(originOldTableIdentifier, originNewTableIdentifierWithOutSchema)
      if (wasCached) {
        //        Cache With Hive Schema Prefix
        sparkSession.catalog.cacheTable(originNewTableIdentifierWithSchema.quotedString)
      }
    }
    Seq.empty[Row]
  }
}
