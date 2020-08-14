package org.apache.spark.sql.hive.execution.command.create

import org.apache.spark.sql.catalyst.TableIdentifier
import org.apache.spark.sql.catalyst.analysis.{UnresolvedFunction, UnresolvedRelation}
import org.apache.spark.sql.{AnalysisException, Row, SparkSession}
import org.apache.spark.sql.catalyst.catalog.{CatalogStorageFormat, CatalogTable, CatalogTableType, SessionCatalog}
import org.apache.spark.sql.catalyst.expressions.Alias
import org.apache.spark.sql.catalyst.plans.QueryPlan
import org.apache.spark.sql.catalyst.plans.logical.{LogicalPlan, Project}
import org.apache.spark.sql.execution.command.ViewHelper.{checkCyclicViewReference, generateViewProperties}
import org.apache.spark.sql.execution.command._
import org.apache.spark.sql.hive.{KatanaContext, KatanaExtension, CatalogSchemaUtil}
import org.apache.spark.sql.types.MetadataBuilder

import scala.collection.mutable.HashMap

/**
  * @author angers.zhu@gmail.com
  * @date 2019/5/29 15:07
  */
case class KatanaCreateView(delegate: CreateViewCommand,
                            hiveCatalogs: HashMap[String, SessionCatalog])
                           (@transient private val katana: KatanaContext) extends RunnableCommand {

  import ViewHelper._

  override protected def innerChildren: Seq[QueryPlan[_]] = Seq(delegate.child)

  if (delegate.viewType == PersistedView) {
    require(delegate.originalText.isDefined, "'originalText' must be provided to create permanent view")
  }

  if (delegate.allowExisting && delegate.replace) {
    throw new AnalysisException("CREATE VIEW with both IF NOT EXISTS and REPLACE is not allowed.")
  }

  private def isTemporary = delegate.viewType == LocalTempView || delegate.viewType == GlobalTempView

  // Disallows 'CREATE TEMPORARY VIEW IF NOT EXISTS' to be consistent with 'CREATE TEMPORARY TABLE'
  if (delegate.allowExisting && isTemporary) {
    throw new AnalysisException(
      "It is not allowed to define a TEMPORARY view with IF NOT EXISTS.")
  }

  // Temporary view names should NOT contain database prefix like "database.table"
  if (isTemporary && delegate.name.database.isDefined) {
    val database = delegate.name.database.get
    throw new AnalysisException(
      s"It is not allowed to add database prefix `$database` for the TEMPORARY view name.")
  }

  override def run(sparkSession: SparkSession): Seq[Row] = {
    // If the plan cannot be analyzed, throw an exception and don't proceed.
    val qe = sparkSession.sessionState.executePlan(delegate.child)
    qe.assertAnalyzed()
    val analyzedPlan = qe.analyzed

    if (delegate.userSpecifiedColumns.nonEmpty &&
      delegate.userSpecifiedColumns.length != analyzedPlan.output.length) {
      throw new AnalysisException(s"The number of columns produced by the SELECT clause " +
        s"(num: `${analyzedPlan.output.length}`) does not match the number of column names " +
        s"specified by CREATE VIEW (num: `${delegate.userSpecifiedColumns.length}`).")
    }
    val catalog =
      CatalogSchemaUtil.getCatalog(
        delegate.name.catalog,
        hiveCatalogs,
        sparkSession,
        katana)

    // When creating a permanent view, not allowed to reference temporary objects.
    // This should be called after `qe.assertAnalyzed()` (i.e., `child` can be resolved)
    verifyTemporaryObjectsNotExists(delegate.name, sparkSession)


    if (delegate.viewType == LocalTempView) {
      val aliasedPlan = aliasPlan(sparkSession, analyzedPlan)
      catalog.createTempView(delegate.name.table, aliasedPlan, overrideIfExists = delegate.replace)
    } else if (delegate.viewType == GlobalTempView) {
      val aliasedPlan = aliasPlan(sparkSession, analyzedPlan)
      catalog.createGlobalTempView(delegate.name.table, aliasedPlan, overrideIfExists = delegate.replace)
    } else if (catalog.tableExists(delegate.name)) {
      val tableMetadata = catalog.getTableMetadata(delegate.name)
      if (delegate.allowExisting) {
        // Handles `CREATE VIEW IF NOT EXISTS v0 AS SELECT ...`. Does nothing when the target view
        // already exists.
      } else if (tableMetadata.tableType != CatalogTableType.VIEW) {
        throw new AnalysisException(s"${delegate.name} is not a view")
      } else if (delegate.replace) {
        // Detect cyclic view reference on CREATE OR REPLACE VIEW.
        val viewIdent = tableMetadata.identifier
        checkCyclicViewReference(analyzedPlan, Seq(viewIdent), viewIdent)

        // Handles `CREATE OR REPLACE VIEW v0 AS SELECT ...`
        // Nothing we need to retain from the old view, so just drop and create a new one
        catalog.dropTable(viewIdent, ignoreIfNotExists = false, purge = false)
        catalog.createTable(prepareTable(delegate.name, sparkSession, analyzedPlan), ignoreIfExists = false)
      } else {
        // Handles `CREATE VIEW v0 AS SELECT ...`. Throws exception when the target view already
        // exists.
        throw new AnalysisException(
          s"View ${delegate.name} already exists. If you want to update the view definition, " +
            "please use ALTER VIEW AS or CREATE OR REPLACE VIEW AS")
      }
    } else {
      // Create the view if it doesn't exist.
      catalog.createTable(prepareTable(delegate.name, sparkSession, analyzedPlan), ignoreIfExists = false)
    }
    Seq.empty[Row]
  }

  /**
    * Permanent views are not allowed to reference temp objects, including temp function and views
    */
  private def verifyTemporaryObjectsNotExists(originTableIdentifier: TableIdentifier, sparkSession: SparkSession): Unit = {
    if (!isTemporary) {
      // This func traverses the unresolved plan `child`. Below are the reasons:
      // 1) Analyzer replaces unresolved temporary views by a SubqueryAlias with the corresponding
      // logical plan. After replacement, it is impossible to detect whether the SubqueryAlias is
      // added/generated from a temporary view.
      // 2) The temp functions are represented by multiple classes. Most are inaccessible from this
      // package (e.g., HiveGenericUDF).
      delegate.child.collect {
        // Disallow creating permanent views based on temporary views.
        case s: UnresolvedRelation
          if sparkSession.sessionState.catalog.isTemporaryTable(s.tableIdentifier) =>
          throw new AnalysisException(s"Not allowed to create a permanent view ${originTableIdentifier} by " +
            s"referencing a temporary view ${s.tableIdentifier}")
        case other if !other.resolved => other.expressions.flatMap(_.collect {
          // Disallow creating permanent views based on temporary UDFs.
          case e: UnresolvedFunction
            if sparkSession.sessionState.catalog.isTemporaryFunction(e.name) =>
            throw new AnalysisException(s"Not allowed to create a permanent view ${originTableIdentifier} by " +
              s"referencing a temporary function `${e.name}`")
        })
      }
    }
  }

  /**
    * If `userSpecifiedColumns` is defined, alias the analyzed plan to the user specified columns,
    * else return the analyzed plan directly.
    */
  private def aliasPlan(session: SparkSession, analyzedPlan: LogicalPlan): LogicalPlan = {
    if (delegate.userSpecifiedColumns.isEmpty) {
      analyzedPlan
    } else {
      val projectList = analyzedPlan.output.zip(delegate.userSpecifiedColumns).map {
        case (attr, (colName, None)) => Alias(attr, colName)()
        case (attr, (colName, Some(colComment))) =>
          val meta = new MetadataBuilder().putString("comment", colComment).build()
          Alias(attr, colName)(explicitMetadata = Some(meta))
      }
      session.sessionState.executePlan(Project(projectList, analyzedPlan)).analyzed
    }
  }

  /**
    * Returns a [[CatalogTable]] that can be used to save in the catalog. Generate the view-specific
    * properties(e.g. view default database, view query output column names) and store them as
    * properties in the CatalogTable, and also creates the proper schema for the view.
    */
  private def prepareTable(originTableIdentifier: TableIdentifier, session: SparkSession, analyzedPlan: LogicalPlan): CatalogTable = {
    if (delegate.originalText.isEmpty) {
      throw new AnalysisException(
        "It is not allowed to create a persisted view from the Dataset API")
    }

    val newProperties = generateViewProperties(delegate.properties, session, analyzedPlan)

    CatalogTable(
      identifier = originTableIdentifier,
      tableType = CatalogTableType.VIEW,
      storage = CatalogStorageFormat.empty,
      schema = aliasPlan(session, analyzedPlan).schema,
      properties = newProperties,
      viewText = delegate.originalText,
      comment = delegate.comment
    )
  }
}
