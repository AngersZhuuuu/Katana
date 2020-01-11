package org.apache.spark.sql.hive.execution.command.drop

import org.apache.spark.sql.{Row, SparkSession}
import org.apache.spark.sql.catalyst.catalog.SessionCatalog
import org.apache.spark.sql.execution.command.{DropDatabaseCommand, RunnableCommand}
import org.apache.spark.sql.hive.{KatanaExtension, CatalogSchemaUtil}

import scala.collection.mutable.HashMap

/**
  * @author angers.zhu@gmail.com
  * @date 2019/5/29 14:12
  */
case class KatanaDropDatabase(delegate: DropDatabaseCommand,
                              hiveCatalogs: HashMap[String, SessionCatalog]) extends RunnableCommand {
  override def run(sparkSession: SparkSession): Seq[Row] = {
    val (catalog: SessionCatalog, originDB: String) =  CatalogSchemaUtil.getCatalogAndOriginDBName(hiveCatalogs, delegate.databaseName)(sparkSession)
    catalog.dropDatabase(originDB, delegate.ifExists, delegate.cascade)
    Seq.empty[Row]
  }
}
