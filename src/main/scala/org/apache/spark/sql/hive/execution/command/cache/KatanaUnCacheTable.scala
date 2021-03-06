package org.apache.spark.sql.hive.execution.command.cache

import org.apache.spark.sql.{Row, SparkSession}
import org.apache.spark.sql.execution.command.{RunnableCommand, UncacheTableCommand}

/**
  * @author angers.zhu@gmail.com
  * @date 2019/5/30 11:04
  */
case class KatanaUnCacheTable(delegate: UncacheTableCommand)
  extends RunnableCommand {

  override def run(sparkSession: SparkSession): Seq[Row] = {
    val tableId = delegate.tableIdent.quotedString
    if (!delegate.ifExists || sparkSession.catalog.tableExists(tableId)) {
      sparkSession.catalog.uncacheTable(tableId)
    }
    Seq.empty[Row]
  }
}
