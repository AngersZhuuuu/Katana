package org.apache.spark.sql.hive.execution.command.show

import org.apache.spark.sql.catalyst.catalog.SessionCatalog
import org.apache.spark.sql.catalyst.expressions.{Attribute, AttributeReference}
import org.apache.spark.sql.execution.command.{RunnableCommand, ShowDatabasesCommand}
import org.apache.spark.sql.types.StringType
import org.apache.spark.sql.{Row, SparkSession}

import scala.collection.mutable.HashMap

/**
  * @author angers.zhu@gmail.com
  * @date 2019/5/28 15:53
  */
case class KatanaShowDatabases(delegate: ShowDatabasesCommand,
                               hiveCatalogs: HashMap[String, SessionCatalog]) extends RunnableCommand {
  override val output: Seq[Attribute] = {
    //    AttributeReference("schemaName", StringType, nullable = false)() ::
    AttributeReference("databaseName", StringType, nullable = false)() :: Nil
  }

  override def run(sparkSession: SparkSession): Seq[Row] = {

    val catalog = sparkSession.sessionState.catalog
    val databases =
      delegate.databasePattern.map(catalog.listDatabases).getOrElse(catalog.listDatabases())

    val externalDatabases: Seq[String] =
      hiveCatalogs.map(schemaAndCatalog => {
        delegate.databasePattern.map(schemaAndCatalog._2.listDatabases).getOrElse(schemaAndCatalog._2.listDatabases)
          .map(schemaAndCatalog._1 + "_" + _)
      }).flatten.toSeq

    (databases ++ externalDatabases).map { d => Row(d) }
  }
}
