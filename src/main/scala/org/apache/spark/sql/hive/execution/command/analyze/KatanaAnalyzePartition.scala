package org.apache.spark.sql.hive.execution.command.analyze

import org.apache.spark.sql.{AnalysisException, Column, Row, SparkSession}
import org.apache.spark.sql.catalyst.TableIdentifier
import org.apache.spark.sql.catalyst.analysis.{NoSuchPartitionException, UnresolvedAttribute}
import org.apache.spark.sql.catalyst.catalog.{CatalogTable, CatalogTableType, ExternalCatalogUtils}
import org.apache.spark.sql.catalyst.catalog.CatalogTypes.TablePartitionSpec
import org.apache.spark.sql.catalyst.expressions.{And, EqualTo, Literal}
import org.apache.spark.sql.execution.command.{AnalyzePartitionCommand, RunnableCommand}
import org.apache.spark.sql.execution.datasources.PartitioningUtils
import org.apache.spark.sql.hive.{CatalogSchemaUtil, KatanaContext}
import org.apache.spark.sql.hive.execution.command.KatanaCommandUtils

/**
 * @author angers.zhu@gmail.com
 * @date 2019/5/30 9:19
 */
case class KatanaAnalyzePartition(
    delegate: AnalyzePartitionCommand)
    (@transient private val katana: KatanaContext)
  extends RunnableCommand {

  private def getPartitionSpec(table: CatalogTable): Option[TablePartitionSpec] = {
    val normalizedPartitionSpec =
      PartitioningUtils.normalizePartitionSpec(delegate.partitionSpec, table.partitionColumnNames,
        table.identifier.quotedString, conf.resolver)

    // Report an error if partition columns in partition specification do not form
    // a prefix of the list of partition columns defined in the table schema
    val isNotSpecified =
    table.partitionColumnNames.map(normalizedPartitionSpec.getOrElse(_, None).isEmpty)
    if (isNotSpecified.init.zip(isNotSpecified.tail).contains((true, false))) {
      val tableId = table.identifier
      val schemaColumns = table.partitionColumnNames.mkString(",")
      val specColumns = normalizedPartitionSpec.keys.mkString(",")
      throw new AnalysisException("The list of partition columns with values " +
        s"in partition specification for table '${tableId.table}' " +
        s"in database '${tableId.database.get}' is not a prefix of the list of " +
        "partition columns defined in the table schema. " +
        s"Expected a prefix of [${schemaColumns}], but got [${specColumns}].")
    }

    val filteredSpec = normalizedPartitionSpec.filter(_._2.isDefined).mapValues(_.get)
    if (filteredSpec.isEmpty) {
      None
    } else {
      Some(filteredSpec)
    }
  }

  override def run(sparkSession: SparkSession): Seq[Row] = {
    val session = CatalogSchemaUtil.getSession(delegate.tableIdent.catalog, sparkSession, katana)
    val catalog = session.sessionState.catalog

    val tableMeta = catalog.getTableMetadata(delegate.tableIdent)
    if (tableMeta.tableType == CatalogTableType.VIEW) {
      throw new AnalysisException("ANALYZE TABLE is not supported on views.")
    }

    val partitionValueSpec = getPartitionSpec(tableMeta)

    val partitions = catalog.listPartitions(tableMeta.identifier, partitionValueSpec)

    if (partitions.isEmpty) {
      if (partitionValueSpec.isDefined) {
        throw new NoSuchPartitionException(delegate.tableIdent.database.get, delegate.tableIdent.table, partitionValueSpec.get)
      } else {
        // the user requested to analyze all partitions for a table which has no partitions
        // return normally, since there is nothing to do
        return Seq.empty[Row]
      }
    }

    // Compute statistics for individual partitions
    val rowCounts: Map[TablePartitionSpec, BigInt] =
      if (delegate.noscan) {
        Map.empty
      } else {
        calculateRowCountsPerPartition(sparkSession, delegate.tableIdent, tableMeta, partitionValueSpec)
      }

    // Update the metastore if newly computed statistics are different from those
    // recorded in the metastore.
    val newPartitions = partitions.flatMap { p =>
      val newTotalSize = KatanaCommandUtils.calculateLocationSize(
        session.sessionState, delegate.tableIdent, p.storage.locationUri)
      val newRowCount = rowCounts.get(p.spec)
      val newStats = KatanaCommandUtils.compareAndGetNewStats(tableMeta.stats, newTotalSize, newRowCount)
      newStats.map(_ => p.copy(stats = newStats))
    }

    if (newPartitions.nonEmpty) {
      catalog.alterPartitions(tableMeta.identifier, newPartitions)
    }

    Seq.empty[Row]
  }

  private def calculateRowCountsPerPartition(
      sparkSession: SparkSession,
      tableIdentifierWithSchema: TableIdentifier,
      tableMeta: CatalogTable,
      partitionValueSpec: Option[TablePartitionSpec]): Map[TablePartitionSpec, BigInt] = {
    val filter = if (partitionValueSpec.isDefined) {
      val filters = partitionValueSpec.get.map {
        case (columnName, value) => EqualTo(UnresolvedAttribute(columnName), Literal(value))
      }
      filters.reduce(And)
    } else {
      Literal.TrueLiteral
    }

    val tableDf = sparkSession.table(tableIdentifierWithSchema)
    val partitionColumns = tableMeta.partitionColumnNames.map(Column(_))

    val df = tableDf.filter(Column(filter)).groupBy(partitionColumns: _*).count()

    df.collect().map { r =>
      val partitionColumnValues = partitionColumns.indices.map { i =>
        if (r.isNullAt(i)) {
          ExternalCatalogUtils.DEFAULT_PARTITION_NAME
        } else {
          r.get(i).toString
        }
      }
      val spec = tableMeta.partitionColumnNames.zip(partitionColumnValues).toMap
      val count = BigInt(r.getLong(partitionColumns.size))
      (spec, count)
    }.toMap
  }
}
