package org.apache.spark.sql.hive.execution.command.alter.database

import org.apache.spark.sql.{Row, SparkSession}
import org.apache.spark.sql.catalyst.catalog.{CatalogDatabase, SessionCatalog}
import org.apache.spark.sql.execution.command.{AlterDatabasePropertiesCommand, RunnableCommand}
import org.apache.spark.sql.hive.CatalogSchemaUtil

import scala.collection.mutable.HashMap

/**
  * @author angers.zhu@gmail.com
  * @date 2019/5/30 16:53
  */
case class KatanaAlterDatabaseProperties(delegate: AlterDatabasePropertiesCommand,
                                         hiveCatalogs: HashMap[String, SessionCatalog]) extends RunnableCommand {
  override def run(sparkSession: SparkSession): Seq[Row] = {
    val (catalog: SessionCatalog, originDB: String) = CatalogSchemaUtil.getCatalogAndOriginDBName(hiveCatalogs, delegate.databaseName)(sparkSession)

    val db: CatalogDatabase = catalog.getDatabaseMetadata(originDB)
    catalog.alterDatabase(db.copy(properties = db.properties ++ delegate.props))

    Seq.empty[Row]
  }
}
