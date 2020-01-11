package org.apache.spark.sql.hive.parser

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.catalyst.analysis.UnresolvedRelation
import org.apache.spark.sql.catalyst.catalog.{CatalogTable, SessionCatalog}
import org.apache.spark.sql.catalyst.expressions.{Expression, SubqueryExpression}
import org.apache.spark.sql.catalyst.parser.ParserInterface
import org.apache.spark.sql.catalyst.plans.logical.{InsertIntoTable, LogicalPlan, SubqueryAlias, With}
import org.apache.spark.sql.catalyst.{FunctionIdentifier, TableIdentifier}
import org.apache.spark.sql.execution.SparkSqlParser
import org.apache.spark.sql.execution.command._
import org.apache.spark.sql.execution.datasources.CreateTable
import org.apache.spark.sql.hive.KatanaContext
import org.apache.spark.sql.types.{DataType, StructType}

import scala.collection.mutable.HashMap

/**
  * @author angers.zhu@gmail.com
  * @date 2019/5/31 14:02
  */
case class KatanaIdentifierParser(getOrCreateKatanaContext: SparkSession => KatanaContext)
                                 (sparkSession: SparkSession,
                                  delegate: ParserInterface) extends ParserInterface {

  private val katanaContext: KatanaContext = getOrCreateKatanaContext(sparkSession)
  private val hiveCatalogs: HashMap[String, SessionCatalog] = katanaContext.hiveCatalogs
  private lazy val internal = new SparkSqlParser(sparkSession.sqlContext.conf)

  def qualifyTableIdentifierInternal(tableIdentifier: TableIdentifier): TableIdentifier = {
    val catalog: SessionCatalog =
      if (katanaContext.getActiveSessionState() == null)
        sparkSession.sessionState.catalog
      else
        katanaContext.getActiveSessionState().catalog
    val originDB = catalog.getCurrentDatabase
    val hiveSchema = hiveCatalogs.find(_._2 == catalog)
    val withSchemaDB = if (hiveSchema.isDefined) hiveSchema.get._1 + "_" + originDB else originDB
    TableIdentifier(
      tableIdentifier.table,
      Some(withSchemaDB)
    )
  }

  def needQualify(tableIdentifier: TableIdentifier): Boolean = tableIdentifier.database.isEmpty

  private val qualifyTableIdentifier: PartialFunction[LogicalPlan, LogicalPlan] = {
    case r@UnresolvedRelation(tableIdentifier) if needQualify(tableIdentifier) =>
      r.copy(qualifyTableIdentifierInternal(tableIdentifier))
    case i@InsertIntoTable(r@UnresolvedRelation(tableIdentifier), _, _, _, _)
      if needQualify(tableIdentifier) =>
      // When getting temp view, we leverage legacy catalog.
      i.copy(table = r.copy(tableIdentifier = qualifyTableIdentifierInternal(tableIdentifier)))
    case w@With(_, cteRelations) =>
      w.copy(
        cteRelations = cteRelations
          .map(p => (p._1, p._2.transform(qualifyTableIdentifier).asInstanceOf[SubqueryAlias]))
      )
    case ct@CreateTable(tableDesc, _, _) =>
      val originTableDesc = new CatalogTable(qualifyTableIdentifierInternal(tableDesc.identifier),
        tableDesc.tableType,
        tableDesc.storage,
        tableDesc.schema,
        tableDesc.provider,
        tableDesc.partitionColumnNames,
        tableDesc.bucketSpec,
        tableDesc.owner,
        tableDesc.createTime,
        tableDesc.lastAccessTime,
        tableDesc.createVersion,
        tableDesc.properties,
        tableDesc.stats,
        tableDesc.viewText,
        tableDesc.comment,
        tableDesc.unsupportedFeatures,
        tableDesc.tracksPartitionsInCatalog,
        tableDesc.schemaPreservesCase,
        tableDesc.ignoredProperties)
      ct.copy(tableDesc = originTableDesc)
    case cv@CreateViewCommand(name, _, _, _, _, child, _, _, _) =>
      cv.copy(name = qualifyTableIdentifierInternal(name), child = child transform qualifyTableIdentifier)
    case e@ExplainCommand(plan, _, _, _) =>
      e.copy(logicalPlan = plan transform qualifyTableIdentifier)
    case c@CacheTableCommand(tableIdentifier, plan, _)
      if plan.isEmpty && needQualify(tableIdentifier) =>
      // Caching an unqualified catalog table.
      c.copy(qualifyTableIdentifierInternal(tableIdentifier))
    case c@CacheTableCommand(_, plan, _) if plan.isDefined =>
      c.copy(plan = Some(plan.get transform qualifyTableIdentifier))
    case u@UncacheTableCommand(tableIdentifier, _) if needQualify(tableIdentifier) =>
      // Uncaching an unqualified catalog table.
      u.copy(qualifyTableIdentifierInternal(tableIdentifier))
    case logicalPlan =>
      logicalPlan transformExpressionsUp {
        case s: SubqueryExpression => s.withNewPlan(s.plan transform qualifyTableIdentifier)
      }
  }

  override def parsePlan(sqlText: String): LogicalPlan =
    internal.parsePlan(sqlText).transform(qualifyTableIdentifier)

  override def parseExpression(sqlText: String): Expression =
    internal.parseExpression(sqlText)

  override def parseTableIdentifier(sqlText: String): TableIdentifier =
    internal.parseTableIdentifier(sqlText)

  override def parseFunctionIdentifier(sqlText: String): FunctionIdentifier =
    internal.parseFunctionIdentifier(sqlText)

  override def parseTableSchema(sqlText: String): StructType =
    internal.parseTableSchema(sqlText)

  override def parseDataType(sqlText: String): DataType =
    internal.parseDataType(sqlText)
}
