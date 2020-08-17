package org.apache.spark.sql.hive.analyze

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.catalyst.analysis.{EliminateSubqueryAliases, NoSuchDatabaseException, NoSuchTableException, UnresolvedRelation}
import org.apache.spark.sql.catalyst.catalog.UnresolvedCatalogRelation
import org.apache.spark.sql.catalyst.plans.logical.{InsertIntoTable, LogicalPlan, SubqueryAlias}
import org.apache.spark.sql.catalyst.rules.Rule
import org.apache.spark.sql.hive.{CatalogSchemaUtil, KatanaContext}

/**
  * @author angers.zhu@gmail.com
  * @date 2019/5/28 9:51
  */
case class KatanaHiveRelationRule(getOrCreateKatanaContext: SparkSession => KatanaContext)
                                 (sparkSession: SparkSession)
  extends Rule[LogicalPlan] {
  private val katanaContext: KatanaContext = getOrCreateKatanaContext(sparkSession)

  override def apply(plan: LogicalPlan): LogicalPlan = {
    plan transformUp resolveHiveSchema
  }

  protected def resolveHiveSchema: PartialFunction[LogicalPlan, LogicalPlan] = {
    case i@InsertIntoTable(u: UnresolvedRelation, _, _, _, _) =>
      i.copy(table = EliminateSubqueryAliases(lookupTableFromCatalog(u)))

    case u@UnresolvedRelation(_) =>
      lookupTableFromCatalog(u)
  }

  private def lookupTableFromCatalog(u: UnresolvedRelation): LogicalPlan = {
    val catalog = CatalogSchemaUtil.getCatalog(u.tableIdentifier.catalog, sparkSession, katanaContext)
    try {
      // 处理relation 同时补全catalog信息
      catalog.lookupRelation(u.tableIdentifier) transformDown {
        case sa@SubqueryAlias(alias, relation: UnresolvedCatalogRelation) =>
          sa.copy(child = relation.copy(tableMeta = relation.tableMeta.copy(u.tableIdentifier)))
      }
    } catch {
      case e: NoSuchTableException =>
        u.failAnalysis(s"Table or view note found: ${u.tableIdentifier.unquotedString}", e)
      // If the database is defined and that database is not found, throw an AnalysisException.
      // Note that if the database is not defined, it is possible we are looking up a temp view.
      case e: NoSuchDatabaseException =>
        u.failAnalysis(s"Table or view not found: ${u.tableIdentifier.unquotedString}, the " +
          s"database ${e.db} doesn't exist.", e)
    }
  }
}
