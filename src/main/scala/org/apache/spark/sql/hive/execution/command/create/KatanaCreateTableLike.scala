package org.apache.spark.sql.hive.execution.command.create

import org.apache.spark.sql.{Row, SparkSession}
import org.apache.spark.sql.catalyst.catalog.{CatalogTable, CatalogTableType, CatalogUtils}
import org.apache.spark.sql.execution.command.{CreateTableLikeCommand, RunnableCommand}
import org.apache.spark.sql.hive.{CatalogSchemaUtil, KatanaContext}

/**
  * @author angers.zhu@gmail.com
  * @date 2019/5/29 14:51
  */
case class KatanaCreateTableLike(delegate: CreateTableLikeCommand)
                                (@transient private val katana: KatanaContext) extends RunnableCommand {
  override def run(sparkSession: SparkSession): Seq[Row] = {
    val catalogTarget =
      CatalogSchemaUtil.getCatalog(
        delegate.targetTable.catalog,
        sparkSession,
        katana)

    val catalogSource =
      CatalogSchemaUtil.getCatalog(
        delegate.sourceTable.catalog,
        sparkSession,
        katana)

    val sourceTableDesc = catalogSource.getTempViewOrPermanentTableMetadata(delegate.sourceTable)

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
        identifier = delegate.targetTable,
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
