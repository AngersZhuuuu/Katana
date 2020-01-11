package org.apache.spark.sql.hive.execution.command.alter.view

import org.apache.spark.sql.catalyst.TableIdentifier
import org.apache.spark.sql.catalyst.catalog.{CatalogTableType, SessionCatalog}
import org.apache.spark.sql.catalyst.plans.QueryPlan
import org.apache.spark.sql.catalyst.plans.logical.LogicalPlan
import org.apache.spark.sql.execution.command.{AlterViewAsCommand, RunnableCommand, ViewHelper}
import org.apache.spark.sql.hive.{KatanaContext, CatalogSchemaUtil}
import org.apache.spark.sql.{AnalysisException, Row, SparkSession}

import scala.collection.mutable.HashMap

/**
  * @author angers.zhu@gmail.com
  * @date 2019/5/30 17:22
  */
case class KatanaAlterViewAs(delegate: AlterViewAsCommand,
                             hiveCatalogs: HashMap[String, SessionCatalog])
                            (@transient private val katana: KatanaContext) extends RunnableCommand {

  import ViewHelper._

  override protected def innerChildren: Seq[QueryPlan[_]] = Seq(delegate.query)

  override def run(sparkSession: SparkSession): Seq[Row] = {
    val (catalog: SessionCatalog, originDB: String) = delegate.name.database match {
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

    val originOldTableIdentifier = new TableIdentifier(delegate.name.table, Some(originDB))
    // If the plan cannot be analyzed, throw an exception and don't proceed.
    val qe = sparkSession.sessionState.executePlan(delegate.query)
    qe.assertAnalyzed()
    val analyzedPlan = qe.analyzed

    if (catalog.alterTempViewDefinition(originOldTableIdentifier, analyzedPlan)) {
      // a local/global temp view has been altered, we are done.
    } else {
      alterPermanentView(catalog, originOldTableIdentifier, sparkSession, analyzedPlan)
    }

    Seq.empty[Row]
  }

  private def alterPermanentView(catalog: SessionCatalog, originOldTableIdentifier: TableIdentifier, session: SparkSession, analyzedPlan: LogicalPlan): Unit = {
    val viewMeta = catalog.getTableMetadata(originOldTableIdentifier)
    if (viewMeta.tableType != CatalogTableType.VIEW) {
      throw new AnalysisException(s"${viewMeta.identifier} is not a view.")
    }

    // Detect cyclic view reference on ALTER VIEW.
    val viewIdent = viewMeta.identifier
    checkCyclicViewReference(analyzedPlan, Seq(viewIdent), viewIdent)

    val newProperties = generateViewProperties(viewMeta.properties, session, analyzedPlan)

    val updatedViewMeta = viewMeta.copy(
      schema = analyzedPlan.schema,
      properties = newProperties,
      viewText = Some(delegate.originalText))

    catalog.alterTable(updatedViewMeta)
  }
}
