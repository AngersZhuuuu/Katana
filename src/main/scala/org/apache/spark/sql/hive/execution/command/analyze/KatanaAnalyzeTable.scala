package org.apache.spark.sql.hive.execution.command.analyze

import org.apache.spark.sql.catalyst.TableIdentifier
import org.apache.spark.sql.catalyst.catalog.{CatalogTableType, SessionCatalog}
import org.apache.spark.sql.execution.command.{AnalyzeTableCommand, RunnableCommand}
import org.apache.spark.sql.hive.execution.command.KatanaCommandUtils
import org.apache.spark.sql.hive.{KatanaContext, CatalogSchemaUtil}
import org.apache.spark.sql.internal.SessionState
import org.apache.spark.sql.{AnalysisException, Row, SparkSession}

import scala.collection.mutable.HashMap

/**
  * @author angers.zhu@gmail.com
  * @date 2019/5/30 9:14
  */
case class KatanaAnalyzeTable(delegate: AnalyzeTableCommand,
                              hiveCatalogs: HashMap[String, SessionCatalog])
                             (@transient private val sessionState: SessionState,
                              @transient private val katana: KatanaContext) extends RunnableCommand {

  override def run(sparkSession: SparkSession): Seq[Row] = {

    val (catalog: SessionCatalog, originDB: String) = delegate.tableIdent.database match {
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
    val originTableIdentifier = new TableIdentifier(delegate.tableIdent.table, Some(originDB))

    /**
      * 带有Schema的DB 用于在SparkSession 生成 UnresolvedRelation 进行路由
      */
    val hiveSchema = hiveCatalogs.find(_._2 == catalog)
    val withSchemaDB = if (hiveSchema.isDefined) hiveSchema.get._1 + "_" + originDB else originDB
    val tableIdentifierWithSchema = new TableIdentifier(delegate.tableIdent.table, Some(withSchemaDB))

    val tableMeta = catalog.getTableMetadata(originTableIdentifier)
    if (tableMeta.tableType == CatalogTableType.VIEW) {
      throw new AnalysisException("ANALYZE TABLE is not supported on views.")
    }

    // Compute stats for the whole table
    val newTotalSize = KatanaCommandUtils.calculateTotalSize(catalog, tableIdentifierWithSchema, sessionState, sparkSession, tableMeta)
    val newRowCount =
      if (delegate.noscan) None else Some(BigInt(sparkSession.table(tableIdentifierWithSchema).count()))

    // Update the metastore if the above statistics of the table are different from those
    // recorded in the metastore.
    val newStats = KatanaCommandUtils.compareAndGetNewStats(tableMeta.stats, newTotalSize, newRowCount)
    if (newStats.isDefined) {
      catalog.alterTableStats(originTableIdentifier, newStats)
    }

    Seq.empty[Row]
  }
}
