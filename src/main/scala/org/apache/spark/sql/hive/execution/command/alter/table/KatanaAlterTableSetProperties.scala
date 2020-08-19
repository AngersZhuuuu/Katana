package org.apache.spark.sql.hive.execution.command.alter.table

import org.apache.spark.sql.{Row, SparkSession}
import org.apache.spark.sql.execution.command.{AlterTableSetPropertiesCommand, DDLUtils, RunnableCommand}
import org.apache.spark.sql.hive.{CatalogSchemaUtil, KatanaContext}

/**
  * @author angers.zhu@gmail.com
  * @date 2019/5/30 17:17
  */
case class KatanaAlterTableSetProperties(delegate: AlterTableSetPropertiesCommand)
                                        (@transient private val katana: KatanaContext) extends RunnableCommand {

  override def run(sparkSession: SparkSession): Seq[Row] = {
    val catalog = CatalogSchemaUtil.getCatalog(delegate.tableName.catalog, sparkSession, katana)

    val table = catalog.getTableMetadata(delegate.tableName)
    DDLUtils.verifyAlterTableType(catalog, table, delegate.isView)
    // This overrides old properties and update the comment parameter of CatalogTable
    // with the newly added/modified comment since CatalogTable also holds comment as its
    // direct property.
    val newTable = table.copy(
      properties = table.properties ++ delegate.properties,
      comment = delegate.properties.get("comment").orElse(table.comment))
    catalog.alterTable(newTable)
    Seq.empty[Row]
  }
}
