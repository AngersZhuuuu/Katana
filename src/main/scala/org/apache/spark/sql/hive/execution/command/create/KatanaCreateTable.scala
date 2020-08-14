package org.apache.spark.sql.hive.execution.command.create

import org.apache.spark.sql.catalyst.TableIdentifier
import org.apache.spark.sql.{Row, SparkSession}
import org.apache.spark.sql.catalyst.catalog.{CatalogTable, SessionCatalog}
import org.apache.spark.sql.execution.command.{CreateTableCommand, RunnableCommand}
import org.apache.spark.sql.hive.{KatanaContext, KatanaExtension, CatalogSchemaUtil}

import scala.collection.mutable.HashMap

/**
  * @author angers.zhu@gmail.com
  * @date 2019/5/29 14:47
  */
case class KatanaCreateTable(delegate: CreateTableCommand,
                             hiveCatalogs: HashMap[String, SessionCatalog])
                            (@transient private val katana: KatanaContext) extends RunnableCommand {
  override def run(sparkSession: SparkSession): Seq[Row] = {
    val catalog =
      CatalogSchemaUtil.getCatalog(
        delegate.table.identifier.catalog,
        hiveCatalogs,
        sparkSession,
        katana)

    catalog.createTable(delegate.table, delegate.ignoreIfExists)
    Seq.empty[Row]
  }
}
