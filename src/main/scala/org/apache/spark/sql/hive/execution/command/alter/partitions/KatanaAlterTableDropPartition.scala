package org.apache.spark.sql.hive.execution.command.alter.partitions

import org.apache.spark.sql.{Row, SparkSession}
import org.apache.spark.sql.execution.command.{AlterTableDropPartitionCommand, DDLUtils, RunnableCommand}
import org.apache.spark.sql.execution.datasources.PartitioningUtils
import org.apache.spark.sql.hive.{CatalogSchemaUtil, KatanaContext}
import org.apache.spark.sql.hive.execution.command.KatanaCommandUtils

/**
  * @author angers.zhu@gmail.com
  * @date 2019/5/30 18:17
  */
case class KatanaAlterTableDropPartition(
    delegate: AlterTableDropPartitionCommand)
    (@transient private val katana: KatanaContext)
  extends RunnableCommand {

  override def run(sparkSession: SparkSession): Seq[Row] = {
    val session = CatalogSchemaUtil.getSession(delegate.tableName.catalog, sparkSession, katana)
    val catalog = session.sessionState.catalog

    val table = catalog.getTableMetadata(delegate.tableName)
    DDLUtils.verifyAlterTableType(catalog, table, isView = false)
    DDLUtils.verifyPartitionProviderIsHive(sparkSession, table, "ALTER TABLE DROP PARTITION")

    val normalizedSpecs = delegate.specs.map { spec =>
      PartitioningUtils.normalizePartitionSpec(
        spec,
        table.partitionColumnNames,
        table.identifier.quotedString,
        sparkSession.sessionState.conf.resolver)
    }

    catalog.dropPartitions(
      table.identifier, normalizedSpecs, ignoreIfNotExists = delegate.ifExists, purge = delegate.purge,
      retainData = delegate.retainData)

    KatanaCommandUtils.updateTableStats(catalog, session, sparkSession, table)

    Seq.empty[Row]
  }
}
