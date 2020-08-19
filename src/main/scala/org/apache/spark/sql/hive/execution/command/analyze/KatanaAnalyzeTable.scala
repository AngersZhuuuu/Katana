package org.apache.spark.sql.hive.execution.command.analyze

import org.apache.spark.sql.{AnalysisException, Row, SparkSession}
import org.apache.spark.sql.catalyst.catalog.CatalogTableType
import org.apache.spark.sql.execution.command.{AnalyzeTableCommand, RunnableCommand}
import org.apache.spark.sql.hive.{CatalogSchemaUtil, KatanaContext}
import org.apache.spark.sql.hive.execution.command.KatanaCommandUtils

/**
  * @author angers.zhu@gmail.com
  * @date 2019/5/30 9:14
  */
case class KatanaAnalyzeTable(delegate: AnalyzeTableCommand)
                             (@transient private val katana: KatanaContext)
  extends RunnableCommand {

  override def run(sparkSession: SparkSession): Seq[Row] = {
    val session = CatalogSchemaUtil.getSession(delegate.tableIdent.catalog, sparkSession, katana)
    val catalog = session.sessionState.catalog

    val tableMeta = catalog.getTableMetadata(delegate.tableIdent)
    if (tableMeta.tableType == CatalogTableType.VIEW) {
      throw new AnalysisException("ANALYZE TABLE is not supported on views.")
    }

    // Compute stats for the whole table
    val newTotalSize =
      KatanaCommandUtils.calculateTotalSize(
        catalog,
        delegate.tableIdent,
        session.sessionState,
        sparkSession,
        tableMeta)
    val newRowCount =
      if (delegate.noscan) None else Some(BigInt(sparkSession.table(delegate.tableIdent).count()))

    // Update the metastore if the above statistics of the table are different from those
    // recorded in the metastore.
    val newStats = KatanaCommandUtils.compareAndGetNewStats(tableMeta.stats, newTotalSize, newRowCount)
    if (newStats.isDefined) {
      catalog.alterTableStats(delegate.tableIdent, newStats)
    }

    Seq.empty[Row]
  }
}
