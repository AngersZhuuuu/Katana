package org.apache.spark.sql.hive.execution.command.cache

import org.apache.spark.sql.{Dataset, Row, SparkSession}
import org.apache.spark.sql.catalyst.plans.QueryPlan
import org.apache.spark.sql.execution.command.{CacheTableCommand, RunnableCommand}

/**
  * @author angers.zhu@gmail.com
  * @date 2019/5/30 11:04
  */
case class KatanaCacheTable(delegate: CacheTableCommand)
  extends RunnableCommand {

  require(delegate.plan.isEmpty || delegate.tableIdent.database.isEmpty,
    "Database name is not allowed in CACHE TABLE AS SELECT")

  override protected def innerChildren: Seq[QueryPlan[_]] = delegate.plan.toSeq

  override def run(sparkSession: SparkSession): Seq[Row] = {
    delegate.plan.foreach { logicalPlan =>
      Dataset.ofRows(sparkSession, logicalPlan).createTempView(delegate.tableIdent.quotedString)
    }
    sparkSession.catalog.cacheTable(delegate.tableIdent.quotedString)

    if (!delegate.isLazy) {
      // Performs eager caching
      sparkSession.table(delegate.tableIdent).count()
    }

    Seq.empty[Row]
  }
}
