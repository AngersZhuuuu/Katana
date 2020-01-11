package org.apache.spark.sql.hive.analyze

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.catalyst.TableIdentifier
import org.apache.spark.sql.catalyst.analysis.{EliminateSubqueryAliases, NoSuchDatabaseException, NoSuchTableException, UnresolvedRelation}
import org.apache.spark.sql.catalyst.catalog.SessionCatalog
import org.apache.spark.sql.catalyst.plans.logical.{InsertIntoTable, LogicalPlan}
import org.apache.spark.sql.catalyst.rules.Rule
import org.apache.spark.sql.hive.{KatanaContext, CatalogSchemaUtil}

import scala.collection.mutable._

/**
  * @author angers.zhu@gmail.com
  * @date 2019/5/28 9:51
  */
case class KatanaHiveRelationRule(getOrCreateKatanaContext: SparkSession => KatanaContext)
                                 (sparkSession: SparkSession)
  extends Rule[LogicalPlan] {
  private val katanaContext: KatanaContext = getOrCreateKatanaContext(sparkSession)
  private val hiveCatalogs: HashMap[String, SessionCatalog] = katanaContext.hiveCatalogs

  override def apply(plan: LogicalPlan): LogicalPlan = {
    plan transformUp resolveHiveSchema
  }

  protected def resolveHiveSchema: PartialFunction[LogicalPlan, LogicalPlan] = {
    case i@InsertIntoTable(u: UnresolvedRelation, _, _, _, _)
      //      if (u.tableIdentifier.database.isDefined && resolveHiveSchemaWithDB(u.tableIdentifier).isDefined)
    =>
      i.copy(table = EliminateSubqueryAliases(lookupTableFromCatalog(u)))

    case u@UnresolvedRelation(_)
      //      if (u.tableIdentifier.database.isDefined && resolveHiveSchemaWithDB(u.tableIdentifier).isDefined)
    =>
      lookupTableFromCatalog(u)
  }

  //  private def lookupTableFromCatalog(u: UnresolvedRelation): LogicalPlan = {
  //    val hiveSchema = resolveHiveSchemaWithDB(u.tableIdentifier)
  //    val catalog: SessionCatalog = hiveCatalogs.get(hiveSchema.get).get
  //    val tableIdentWithDb = u.tableIdentifier.copy(database =
  //      Some(SchemaUtil.sliceHiveSchema(u.tableIdentifier.database.get, hiveSchema.get)))
  //    try {
  //      catalog.lookupRelation(tableIdentWithDb)
  //    } catch {
  //      case e: NoSuchTableException =>
  //        u.failAnalysis(s"Table or view not found: ${tableIdentWithDb.unquotedString}", e)
  //      // If the database is defined and that database is not found, throw an AnalysisException.
  //      // Note that if the database is not defined, it is possible we are looking up a temp view.
  //      case e: NoSuchDatabaseException =>
  //        u.failAnalysis(s"Table or view not found: ${tableIdentWithDb.unquotedString}, the " +
  //          s"database ${e.db} doesn't exist.", e)
  //    }
  //  }

  private def lookupTableFromCatalog(u: UnresolvedRelation): LogicalPlan = {
    val (catalog, tableIdentifier) = resolveRelation(u.tableIdentifier)
    try {
      catalog.lookupRelation(tableIdentifier)
    } catch {
      case e: NoSuchTableException =>
        u.failAnalysis(s"Table or view note found: ${tableIdentifier.unquotedString}", e)
      // If the database is defined and that database is not found, throw an AnalysisException.
      // Note that if the database is not defined, it is possible we are looking up a temp view.
      case e: NoSuchDatabaseException =>
        u.failAnalysis(s"Table or view not found: ${tableIdentifier.unquotedString}, the " +
          s"database ${e.db} doesn't exist.", e)
    }
  }

  def resolveHiveSchemaWithDB(tableIdentifier: TableIdentifier): Option[String] = {
    val db = tableIdentifier.database.get
    hiveCatalogs.keySet.find(db.startsWith(_))
  }

  def resolveRelation(tableIdentifier: TableIdentifier): (SessionCatalog, TableIdentifier) = {
    val (catalog: SessionCatalog, originDB: String) = tableIdentifier.database match {
      case None => {
        val tempCatalog =
          if (katanaContext.getActiveSessionState() == null)
            sparkSession.sessionState.catalog
          else
            katanaContext.getActiveSessionState().catalog
        (tempCatalog, tempCatalog.getCurrentDatabase)
      }
      case Some(db) => CatalogSchemaUtil.getCatalogAndOriginDBName(hiveCatalogs, db)(sparkSession)
    }

    val originTableIdentifier = new TableIdentifier(tableIdentifier.table, Some(originDB))
    (catalog, originTableIdentifier)
  }
}
