package org.apache.spark.sql.hive.execution.command.create

import org.apache.spark.sql.catalyst.catalog.{CatalogDatabase, CatalogUtils, SessionCatalog}
import org.apache.spark.sql.execution.command.{CreateDatabaseCommand, RunnableCommand}
import org.apache.spark.sql.hive.CatalogSchemaUtil
import org.apache.spark.sql.{Row, SparkSession}

import scala.collection.mutable.HashMap

/**
  * @author angers.zhu@gmail.com
  * @date 2019/5/28 18:37
  */
case class KatanaCreateDatabase(delegate: CreateDatabaseCommand,
                                hiveCatalogs: HashMap[String, SessionCatalog]) extends RunnableCommand {
  override def run(sparkSession: SparkSession): Seq[Row] = {
    val (catalog: SessionCatalog, db: String) = CatalogSchemaUtil.getCatalogAndOriginDBName(hiveCatalogs, delegate.databaseName)(sparkSession)
    catalog.createDatabase(
      CatalogDatabase(
        db,
        delegate.comment.getOrElse(""),
        //        TODO  ADD HDFS-SERVER
        delegate.path.map(CatalogUtils.stringToURI).getOrElse(catalog.getDefaultDBPath(db)),
        delegate.props),
      delegate.ifNotExists)
    Seq.empty[Row]
  }
}
