package org.apache.spark.sql.hive.execution.command.show

import org.apache.spark.sql.{Row, SparkSession}
import org.apache.spark.sql.catalyst.expressions.{Attribute, AttributeReference}
import org.apache.spark.sql.execution.command.{RunnableCommand, ShowDatabasesCommand}
import org.apache.spark.sql.hive.{CatalogSchemaUtil, KatanaContext}
import org.apache.spark.sql.types.StringType

/**
 * A command for users to list the databases/schemas.
 * If a databasePattern is supplied then the databases that only match the
 * pattern would be listed.
 * The syntax of using this command in SQL is:
 * {{{
 *     SHOW (DATABASES|SCHEMAS) [(IN|FROM) CATALOG] [LIKE 'identifier_with_wildcards'];
 * }}}
 *
 * Different raw Spark SQL, you can show DATABASES specify a spec catalog to only show db from it
 *
 * @author angers.zhu@gmail.com
 * @date 2019/5/28 15:53
 */
case class KatanaShowDatabases(
    delegate: ShowDatabasesCommand)
    (@transient private val katana: KatanaContext)
  extends RunnableCommand {

  override val output: Seq[Attribute] = {
    AttributeReference("catalog", StringType, nullable = false)() ::
      AttributeReference("databaseName", StringType, nullable = false)() :: Nil
  }

  override def run(sparkSession: SparkSession): Seq[Row] = {
    // show all databases respect pattern
    if (delegate.catalog.isEmpty) {
      val defaultCatalog = sparkSession.sessionState.catalog
      val defaultDatabases =
        delegate.databasePattern.map(defaultCatalog.listDatabases).getOrElse(defaultCatalog.listDatabases())
          .map(katana.INTERNAL_HMS_NAME -> _)

      // Current hive catalog map have contain internal hive catalog
      val externalDatabases: Seq[(String, String)] =
        katana.sessions.map { case (name, session) =>
          delegate.databasePattern.map(session.sessionState.catalog.listDatabases)
            .getOrElse(session.sessionState.catalog.listDatabases)
            .map(name -> _)
        }.flatten.toSeq

      (defaultDatabases ++ externalDatabases).map { case (catalog, db) => Row(catalog, db) }
    } else {
      // show spec catalog's databases
      val catalog = CatalogSchemaUtil.getCatalog(delegate.catalog, sparkSession, katana)
      val catalogName = CatalogSchemaUtil.getCatalogName(catalog, katana)
      delegate.databasePattern.map(catalog.listDatabases).getOrElse(catalog.listDatabases())
        .map(catalogName -> _)
        .map { case (catalog, db) => Row(catalog, db) }
    }
  }
}
