package org.apache.spark.sql.hive.execution.command.create

import org.apache.spark.sql.catalyst.TableIdentifier
import org.apache.spark.sql.{Row, SparkSession}
import org.apache.spark.sql.catalyst.catalog.{CatalogTable, SessionCatalog}
import org.apache.spark.sql.execution.command.{CreateTableCommand, RunnableCommand}
import org.apache.spark.sql.hive.{KatanaContext, KatanaExtension, CatalogSchemaUtil}

import scala.collection.mutable.HashMap

/**
  * @author angers.zhu@gmail.com
  * @date 2019/5/29 14:47
  */
case class KatanaCreateTable(delegate: CreateTableCommand,
                             hiveCatalogs: HashMap[String, SessionCatalog])
                            (@transient private val katana: KatanaContext) extends RunnableCommand {
  override def run(sparkSession: SparkSession): Seq[Row] = {
    val (catalog: SessionCatalog, originDB: String) = delegate.table.identifier.database match {
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

    val originTableIdentifier = new TableIdentifier(delegate.table.identifier.table, Some(originDB))

    val originTableDesc = new CatalogTable(originTableIdentifier,
      delegate.table.tableType,
      delegate.table.storage,
      delegate.table.schema,
      delegate.table.provider,
      delegate.table.partitionColumnNames,
      delegate.table.bucketSpec,
      delegate.table.owner,
      delegate.table.createTime,
      delegate.table.lastAccessTime,
      delegate.table.createVersion,
      delegate.table.properties,
      delegate.table.stats,
      delegate.table.viewText,
      delegate.table.comment,
      delegate.table.unsupportedFeatures,
      delegate.table.tracksPartitionsInCatalog,
      delegate.table.schemaPreservesCase,
      delegate.table.ignoredProperties)


    catalog.createTable(originTableDesc, delegate.ignoreIfExists)
    Seq.empty[Row]
  }
}
