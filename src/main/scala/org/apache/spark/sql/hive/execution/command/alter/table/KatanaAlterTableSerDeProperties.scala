package org.apache.spark.sql.hive.execution.command.alter.table

import org.apache.spark.sql.{AnalysisException, Row, SparkSession}
import org.apache.spark.sql.execution.command.{AlterTableSerDePropertiesCommand, DDLUtils, RunnableCommand}
import org.apache.spark.sql.hive.{CatalogSchemaUtil, KatanaContext}

/**
  * @author angers.zhu@gmail.com
  * @date 2019/5/30 17:05
  */
case class KatanaAlterTableSerDeProperties(
    delegate: AlterTableSerDePropertiesCommand)
    (@transient private val katana: KatanaContext)
  extends RunnableCommand {

  // should never happen if we parsed things correctly
  require(delegate.serdeClassName.isDefined || delegate.serdeProperties.isDefined,
    "ALTER TABLE attempted to set neither serde class name nor serde properties")

  override def run(sparkSession: SparkSession): Seq[Row] = {
    val catalog = CatalogSchemaUtil.getCatalog(delegate.tableName.catalog, sparkSession, katana)

    val table = catalog.getTableMetadata(delegate.tableName)
    DDLUtils.verifyAlterTableType(catalog, table, isView = false)
    // For datasource tables, disallow setting serde or specifying partition
    if (delegate.partSpec.isDefined && DDLUtils.isDatasourceTable(table)) {
      throw new AnalysisException("Operation not allowed: ALTER TABLE SET " +
        "[SERDE | SERDEPROPERTIES] for a specific partition is not supported " +
        "for tables created with the datasource API")
    }
    if (delegate.serdeClassName.isDefined && DDLUtils.isDatasourceTable(table)) {
      throw new AnalysisException("Operation not allowed: ALTER TABLE SET SERDE is " +
        "not supported for tables created with the datasource API")
    }
    if (delegate.partSpec.isEmpty) {
      val newTable = table.withNewStorage(
        serde = delegate.serdeClassName.orElse(table.storage.serde),
        properties = table.storage.properties ++ delegate.serdeProperties.getOrElse(Map()))
      catalog.alterTable(newTable)
    } else {
      val spec = delegate.partSpec.get
      val part = catalog.getPartition(table.identifier, spec)
      val newPart = part.copy(storage = part.storage.copy(
        serde = delegate.serdeClassName.orElse(part.storage.serde),
        properties = part.storage.properties ++ delegate.serdeProperties.getOrElse(Map())))
      catalog.alterPartitions(table.identifier, Seq(newPart))
    }
    Seq.empty[Row]
  }
}
