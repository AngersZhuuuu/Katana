package org.apache.spark.sql.hive.execution.command.cache

import org.apache.spark.sql.{Row, SparkSession}
import org.apache.spark.sql.execution.command.{ClearCacheCommand, RunnableCommand}

/**
  * @author angers.zhu@gmail.com
  * @date 2019/5/30 11:05
  */
case class KatanaClearCache(delegate: ClearCacheCommand)
  extends RunnableCommand {

  override def run(sparkSession: SparkSession): Seq[Row] = {
    sparkSession.catalog.clearCache()
    Seq.empty[Row]
  }

  /** [[org.apache.spark.sql.catalyst.trees.TreeNode.makeCopy()]] does not support 0-arg ctor. */
  override def makeCopy(newArgs: Array[AnyRef]): ClearCacheCommand = ClearCacheCommand()
}
