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
    // 不能跨 meta rename table
    if(delegate.oldName.catalog != delegate.newName.catalog) {
      throw new RuntimeException("Katana can't rename table between different catalog")
    }

    val catalog =
      CatalogSchemaUtil.getCatalog(
        delegate.oldName.catalog,
        hiveCatalogs,
        sparkSession,
        katana)

    // If this is a temp view, just rename the view.
    // Otherwise, if this is a real table, we also need to uncache and invalidate the table.
    if (catalog.isTemporaryTable(delegate.oldName)) {
      catalog.renameTable(delegate.oldName, delegate.newName)
    } else {
      val table = catalog.getTableMetadata(delegate.oldName)
      DDLUtils.verifyAlterTableType(catalog, table, delegate.isView)
      // If an exception is thrown here we can just assume the table is uncached;
      // this can happen with Hive tables when the underlying catalog is in-memory.
      val wasCached = Try(sparkSession.catalog.isCached(delegate.oldName.unquotedString)).getOrElse(false)
      if (wasCached) {
        try {
          sparkSession.catalog.uncacheTable(delegate.oldName.unquotedString)
        } catch {
          case NonFatal(e) => log.warn(e.toString, e)
        }
      }
      // Invalidate the table last, otherwise uncaching the table would load the logical plan
      // back into the hive metastore cache
      catalog.refreshTable(delegate.oldName)
      catalog.renameTable(delegate.oldName, delegate.newName)
      if (wasCached) {
        //        Cache With Hive Schema Prefix
        sparkSession.catalog.cacheTable(delegate.newName.quotedString)
      }
    }
    Seq.empty[Row]
  }
}
