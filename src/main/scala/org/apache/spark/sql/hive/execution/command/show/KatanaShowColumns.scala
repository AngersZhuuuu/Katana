package org.apache.spark.sql.hive.execution.command.show

import org.apache.spark.sql.{AnalysisException, Row, SparkSession}
import org.apache.spark.sql.catalyst.expressions.{Attribute, AttributeReference}
import org.apache.spark.sql.execution.command.{RunnableCommand, ShowColumnsCommand}
import org.apache.spark.sql.hive.{CatalogSchemaUtil, KatanaContext}
import org.apache.spark.sql.types.StringType

/**
  * @author angers.zhu@gmail.com
  * @date 2019/5/29 10:12
  */
case class KatanaShowColumns(
   delegate: ShowColumnsCommand)
   (@transient private val katana: KatanaContext)
  extends RunnableCommand {

  override val output: Seq[Attribute] = {
    AttributeReference("col_name", StringType, nullable = false)() :: Nil
  }

  override def run(sparkSession: SparkSession): Seq[Row] = {
    val resolver = sparkSession.sessionState.conf.resolver
    val catalog = (delegate.databaseName, delegate.catalog) match {
      case (None, None) =>
        CatalogSchemaUtil.getCatalog(delegate.tableName.catalog, sparkSession, katana)

      case (Some(db), None) if delegate.tableName.database.exists(!resolver(_, db)) =>
        throw new AnalysisException(
          s"SHOW COLUMNS with conflicting databases: '$db' != '${delegate.tableName.database.get}'")

      case (Some(db), None) =>
        CatalogSchemaUtil.getCatalog(delegate.tableName.catalog, sparkSession, katana)

      case (Some(db), Some(_)) if delegate.tableName.database.exists(!resolver(_, db)) =>
        throw new AnalysisException(
          s"SHOW COLUMNS with conflicting databases: '$db' != '${delegate.tableName.database.get}'")

      case (Some(_), Some(_)) =>
        CatalogSchemaUtil.getCatalog(delegate.catalog, sparkSession, katana)
    }

    val table = catalog.getTempViewOrPermanentTableMetadata(delegate.tableName)
    table.schema.map { c =>
      Row(c.name)
    }
  }
}
