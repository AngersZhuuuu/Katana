package org.apache.spark.sql.hive.execution.command.drop

import org.apache.spark.sql.{AnalysisException, Row, SparkSession}
import org.apache.spark.sql.catalyst.catalog.{CatalogTableType, SessionCatalog}
import org.apache.spark.sql.execution.command.{DropTableCommand, RunnableCommand}
import org.apache.spark.sql.hive.{KatanaContext, CatalogSchemaUtil}

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
    val catalog =
      CatalogSchemaUtil.getCatalog(
        delegate.tableName.catalog,
        hiveCatalogs,
        sparkSession,
        katana)

    val isTempView = catalog.isTemporaryTable(delegate.tableName)

    if (!isTempView && catalog.tableExists(delegate.tableName)) {
      // If the command DROP VIEW is to drop a table or DROP TABLE is to drop a view
      // issue an exception.
      catalog.getTableMetadata(delegate.tableName).tableType match {
        case CatalogTableType.VIEW if !delegate.isView =>
          throw new AnalysisException(
            "Cannot drop a view with DROP TABLE. Please use DROP VIEW instead")
        case o if o != CatalogTableType.VIEW && delegate.isView =>
          throw new AnalysisException(
            s"Cannot drop a table with DROP VIEW. Please use DROP TABLE instead")
        case _ =>
      }
    }

    if (isTempView || catalog.tableExists(delegate.tableName)) {
      try {
        sparkSession.sharedState.cacheManager.uncacheQuery(
          sparkSession.table(delegate.tableName), cascade = !isTempView)
      } catch {
        case NonFatal(e) => log.warn(e.toString, e)
      }
      catalog.refreshTable(delegate.tableName)
      catalog.dropTable(delegate.tableName, delegate.ifExists, delegate.purge)
    } else if (delegate.ifExists) {
      // no-op
    } else {
      throw new AnalysisException(s"Table or view not found: ${delegate.tableName.identifier}")
    }
    Seq.empty[Row]
  }
}
