package org.apache.spark.sql.hive.execution.command.alter.table

import org.apache.spark.sql.catalyst.TableIdentifier
import org.apache.spark.sql.catalyst.catalog.SessionCatalog
import org.apache.spark.sql.execution.command.{AlterTableSerDePropertiesCommand, DDLUtils, RunnableCommand}
import org.apache.spark.sql.hive.{KatanaContext, CatalogSchemaUtil}
import org.apache.spark.sql.{AnalysisException, Row, SparkSession}

import scala.collection.mutable.HashMap

/**
  * @author angers.zhu@gmail.com
  * @date 2019/5/30 17:05
  */
case class KatanaAlterTableSerDeProperties(delegate: AlterTableSerDePropertiesCommand,
                                           hiveCatalogs: HashMap[String, SessionCatalog])
                                          (@transient private val katana: KatanaContext) extends RunnableCommand {

  // should never happen if we parsed things correctly
  require(delegate.serdeClassName.isDefined || delegate.serdeProperties.isDefined,
    "ALTER TABLE attempted to set neither serde class name nor serde properties")

  override def run(sparkSession: SparkSession): Seq[Row] = {

    val (catalog: SessionCatalog, originDB: String) = delegate.tableName.database match {
      case None => {
        val tempCatalog =
          if (katana.getActiveSessionState() == null)
            sparkSession.sessionState.catalog
          else
            katana.getActiveSessionState().catalog
        (tempCatalog, tempCatalog.getCurrentDatabase)
      }
      case Some(db) => CatalogSchemaUtil.getCatalogAndOriginDBName(hiveCatalogs, db)(sparkSession)
    }

    val originOldTableIdentifier = new TableIdentifier(delegate.tableName.table, Some(originDB))

    val table = catalog.getTableMetadata(originOldTableIdentifier)
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
