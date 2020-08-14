package org.apache.spark.sql.hive.execution.command.alter.partitions

import org.apache.spark.sql.catalyst.TableIdentifier
import org.apache.spark.sql.catalyst.catalog.SessionCatalog
import org.apache.spark.sql.execution.command.{AlterTableDropPartitionCommand, DDLUtils, RunnableCommand}
import org.apache.spark.sql.execution.datasources.PartitioningUtils
import org.apache.spark.sql.hive.execution.command.KatanaCommandUtils
import org.apache.spark.sql.hive.{KatanaContext, CatalogSchemaUtil}
import org.apache.spark.sql.internal.SessionState
import org.apache.spark.sql.{Row, SparkSession}

import scala.collection.mutable.HashMap

/**
  * @author angers.zhu@gmail.com
  * @date 2019/5/30 18:17
  */
case class KatanaAlterTableDropPartition(delegate: AlterTableDropPartitionCommand,
                                         hiveCatalogs: HashMap[String, SessionCatalog])
                                        (@transient private val sessionState: SessionState,
                                         @transient private val katana: KatanaContext) extends RunnableCommand {

  override def run(sparkSession: SparkSession): Seq[Row] = {
    val catalog =
      CatalogSchemaUtil.getCatalog(
        delegate.tableName.catalog,
        hiveCatalogs,
        sparkSession,
        katana)

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

    KatanaCommandUtils.updateTableStats(catalog, sessionState, sparkSession, table)

    Seq.empty[Row]
  }
}
