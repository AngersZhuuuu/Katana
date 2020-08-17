package org.apache.spark.sql.hive.execution.command.drop

import org.apache.spark.sql.{Row, SparkSession}
import org.apache.spark.sql.execution.command.{DropDatabaseCommand, RunnableCommand}
import org.apache.spark.sql.hive.{CatalogSchemaUtil, KatanaContext}

/**
  * @author angers.zhu@gmail.com
  * @date 2019/5/29 14:12
  */
case class KatanaDropDatabase(delegate: DropDatabaseCommand)
                             (@transient private val katana: KatanaContext)extends RunnableCommand {
  override def run(sparkSession: SparkSession): Seq[Row] = {
    val catalog = CatalogSchemaUtil.getCatalog(
      delegate.catalog,
      sparkSession,
      katana)
    catalog.dropDatabase(delegate.databaseName, delegate.ifExists, delegate.cascade)
    Seq.empty[Row]
  }
}
