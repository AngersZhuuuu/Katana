package org.apache.spark.sql.hive.strategy

import org.apache.spark.sql.catalyst.catalog.{HiveTableRelation, SessionCatalog}
import org.apache.spark.sql.catalyst.expressions.{Attribute, AttributeSet, Expression, NamedExpression}
import org.apache.spark.sql.catalyst.planning.PhysicalOperation
import org.apache.spark.sql.catalyst.plans.logical.LogicalPlan
import org.apache.spark.sql.execution.{FilterExec, ProjectExec, SparkPlan}
import org.apache.spark.sql.hive.{KatanaContext, CatalogSchemaUtil}
import org.apache.spark.sql.hive.execution.KatanaHiveTableScanExec
import org.apache.spark.sql.{SparkSession, Strategy, catalyst}

import scala.collection.mutable.HashMap

/**
  * @author angers.zhu@gmail.com
  * @date 2019/5/28 13:30
  */
case class KatanaHiveStrategies(getOrCreateKatanaContext: SparkSession => KatanaContext)
                               (sparkSession: SparkSession) extends Strategy {

  private val katanaContext: KatanaContext = getOrCreateKatanaContext(sparkSession)
  private val hiveCatalogs: HashMap[String, SessionCatalog] = katanaContext.hiveCatalogs

  def pruneFilterProject(projectList: Seq[NamedExpression],
                         filterPredicates: Seq[Expression],
                         prunePushedDownFilters: Seq[Expression] => Seq[Expression],
                         scanBuilder: Seq[Attribute] => SparkPlan): SparkPlan = {

    val projectSet = AttributeSet(projectList.flatMap(_.references))
    val filterSet = AttributeSet(filterPredicates.flatMap(_.references))
    val filterCondition: Option[Expression] =
      prunePushedDownFilters(filterPredicates).reduceLeftOption(catalyst.expressions.And)

    // Right now we still use a projection even if the only evaluation is applying an alias
    // to a column.  Since this is a no-op, it could be avoided. However, using this
    // optimization with the current implementation would change the output schema.
    // TODO: Decouple final output schema from expression evaluation so this copy can be
    // avoided safely.

    if (AttributeSet(projectList.map(_.toAttribute)) == projectSet &&
      filterSet.subsetOf(projectSet)) {
      // When it is possible to just use column pruning to get the right projection and
      // when the columns of this projection are enough to evaluate all filter conditions,
      // just do a scan followed by a filter, with no extra project.
      val scan = scanBuilder(projectList.asInstanceOf[Seq[Attribute]])
      filterCondition.map(FilterExec(_, scan)).getOrElse(scan)
    } else {
      val scan = scanBuilder((projectSet ++ filterSet).toSeq)
      ProjectExec(projectList, filterCondition.map(FilterExec(_, scan)).getOrElse(scan))
    }
  }

  override def apply(plan: LogicalPlan): Seq[SparkPlan] = plan match {
    case PhysicalOperation(projectList, predicates, relation: HiveTableRelation) =>
      // Filter out all predicates that only deal with partition keys, these are given to the
      // hive table scan operator to be used for partition pruning.
      val partitionKeyIds = AttributeSet(relation.partitionCols)
      val (pruningPredicates, otherPredicates) = predicates.partition { predicate =>
        !predicate.references.isEmpty &&
          predicate.references.subsetOf(partitionKeyIds)
      }
      val catalog = CatalogSchemaUtil.getCatalog(hiveCatalogs, relation)(sparkSession)
      pruneFilterProject(
        projectList,
        otherPredicates,
        identity[Seq[Expression]],
        KatanaHiveTableScanExec(_, relation, pruningPredicates)(sparkSession, catalog)) :: Nil
    case _ =>
      Nil
  }
}
