package org.apache.spark.sql.hive.execution.command.create

import org.apache.spark.sql.catalyst.TableIdentifier
import org.apache.spark.sql.catalyst.catalog.{CatalogTable, CatalogTableType, CatalogUtils, SessionCatalog}
import org.apache.spark.sql.execution.command.{CreateTableLikeCommand, RunnableCommand}
import org.apache.spark.sql.hive.{KatanaContext, CatalogSchemaUtil}
import org.apache.spark.sql.{Row, SparkSession}

import scala.collection.mutable.HashMap

/**
  * @author angers.zhu@gmail.com
  * @date 2019/5/29 14:51
  */
case class KatanaCreateTableLike(delegate: CreateTableLikeCommand,
                                 hiveCatalogs: HashMap[String, SessionCatalog])
                                (@transient private val katana: KatanaContext) extends RunnableCommand {
  override def run(sparkSession: SparkSession): Seq[Row] = {
    val (catalogTarget: SessionCatalog, originTargetDB: String) = delegate.targetTable.database match {
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

    val originTargetTableIdentifier = new TableIdentifier(delegate.targetTable.table, Some(originTargetDB))


    val (catalogSource: SessionCatalog, originSourceDB: String) = delegate.sourceTable.database match {
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

    val originSourceTableIdentifier = new TableIdentifier(delegate.sourceTable.table, Some(originSourceDB))


    val sourceTableDesc = catalogSource.getTempViewOrPermanentTableMetadata(originSourceTableIdentifier)

    val newProvider = if (sourceTableDesc.tableType == CatalogTableType.VIEW) {
      Some(sparkSession.sessionState.conf.defaultDataSourceName)
    } else {
      sourceTableDesc.provider
    }

    // If the location is specified, we create an external table internally.
    // Otherwise create a managed table.
    val tblType = if (delegate.location.isEmpty) CatalogTableType.MANAGED else CatalogTableType.EXTERNAL

    val newTableDesc =
      CatalogTable(
        identifier = originTargetTableIdentifier,
        tableType = tblType,
        storage = sourceTableDesc.storage.copy(
          locationUri = delegate.location.map(CatalogUtils.stringToURI(_))),
        schema = sourceTableDesc.schema,
        provider = newProvider,
        partitionColumnNames = sourceTableDesc.partitionColumnNames,
        bucketSpec = sourceTableDesc.bucketSpec)

    catalogTarget.createTable(newTableDesc, delegate.ifNotExists)
    Seq.empty[Row]
  }
}
