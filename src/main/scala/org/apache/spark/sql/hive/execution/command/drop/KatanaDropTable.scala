package org.apache.spark.sql.hive.execution.command.drop

import org.apache.spark.sql.catalyst.TableIdentifier
import org.apache.spark.sql.{AnalysisException, Row, SparkSession}
import org.apache.spark.sql.catalyst.catalog.{CatalogTableType, SessionCatalog}
import org.apache.spark.sql.execution.command.{DropTableCommand, RunnableCommand}
import org.apache.spark.sql.hive.{KatanaContext, KatanaExtension, CatalogSchemaUtil}

import scala.collection.mutable.HashMap
import scala.util.control.NonFatal

/**
  * @author angers.zhu@gmail.com
  * @date 2019/5/29 14:15
  */
case class KatanaDropTable(delegate: DropTableCommand,
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
    val originTableIdentifier = new TableIdentifier(table = delegate.tableName.table, database = Some(originDB))

    val isTempView = catalog.isTemporaryTable(originTableIdentifier)

    if (!isTempView && catalog.tableExists(originTableIdentifier)) {
      // If the command DROP VIEW is to drop a table or DROP TABLE is to drop a view
      // issue an exception.
      catalog.getTableMetadata(originTableIdentifier).tableType match {
        case CatalogTableType.VIEW if !delegate.isView =>
          throw new AnalysisException(
            "Cannot drop a view with DROP TABLE. Please use DROP VIEW instead")
        case o if o != CatalogTableType.VIEW && delegate.isView =>
          throw new AnalysisException(
            s"Cannot drop a table with DROP VIEW. Please use DROP TABLE instead")
        case _ =>
      }
    }

    if (isTempView || catalog.tableExists(originTableIdentifier)) {
      try {
        sparkSession.sharedState.cacheManager.uncacheQuery(
          sparkSession.table(originTableIdentifier), cascade = !isTempView)
      } catch {
        case NonFatal(e) => log.warn(e.toString, e)
      }
      catalog.refreshTable(originTableIdentifier)
      catalog.dropTable(originTableIdentifier, delegate.ifExists, delegate.purge)
    } else if (delegate.ifExists) {
      // no-op
    } else {
      throw new AnalysisException(s"Table or view not found: ${originTableIdentifier.identifier}")
    }
    Seq.empty[Row]
  }
}
