package org.apache.spark.sql.hive.execution.command.alter.table

import org.apache.spark.sql.{AnalysisException, Row, SparkSession}
import org.apache.spark.sql.execution.command.{AlterTableUnsetPropertiesCommand, DDLUtils, RunnableCommand}
import org.apache.spark.sql.hive.{CatalogSchemaUtil, KatanaContext}

/**
  * @author angers.zhu@gmail.com
  * @date 2019/5/30 17:20
  */
case class KatanaAlterTableUnsetProperties(delegate: AlterTableUnsetPropertiesCommand)
                                          (@transient private val katana: KatanaContext) extends RunnableCommand {
  override def run(sparkSession: SparkSession): Seq[Row] = {
    val catalog = CatalogSchemaUtil.getCatalog(delegate.tableName.catalog, sparkSession, katana)

    val table = catalog.getTableMetadata(delegate.tableName)
    DDLUtils.verifyAlterTableType(catalog, table, delegate.isView)
    if (!delegate.ifExists) {
      delegate.propKeys.foreach { k =>
        if (!table.properties.contains(k) && k != "comment") {
          throw new AnalysisException(
            s"Attempted to unset non-existent property '$k' in table '${table.identifier}'")
        }
      }
    }
    // If comment is in the table property, we reset it to None
    val tableComment = if (delegate.propKeys.contains("comment")) None else table.comment
    val newProperties = table.properties.filter { case (k, _) => !delegate.propKeys.contains(k) }
    val newTable = table.copy(properties = newProperties, comment = tableComment)
    catalog.alterTable(newTable)
    Seq.empty[Row]
  }
}
