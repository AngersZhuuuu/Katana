package org.apache.spark.sql.hive.execution.command.create

import org.apache.spark.sql.{Row, SparkSession}
import org.apache.spark.sql.execution.command.{CreateTableCommand, RunnableCommand}
import org.apache.spark.sql.hive.{CatalogSchemaUtil, KatanaContext}

/**
  * @author angers.zhu@gmail.com
  * @date 2019/5/29 14:47
  */
case class KatanaCreateTable(delegate: CreateTableCommand)
                            (@transient private val katana: KatanaContext) extends RunnableCommand {
  override def run(sparkSession: SparkSession): Seq[Row] = {
    val catalog =
      CatalogSchemaUtil.getCatalog(
        delegate.table.identifier.catalog,
        sparkSession,
        katana)

    catalog.createTable(delegate.table, delegate.ignoreIfExists)
    Seq.empty[Row]
  }
}
