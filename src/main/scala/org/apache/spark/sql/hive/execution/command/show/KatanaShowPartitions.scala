package org.apache.spark.sql.hive.execution.command.show

import org.apache.spark.sql.{AnalysisException, Row, SparkSession}
import org.apache.spark.sql.catalyst.catalog.CatalogTableType._
import org.apache.spark.sql.catalyst.expressions.{Attribute, AttributeReference}
import org.apache.spark.sql.execution.command.{DDLUtils, RunnableCommand, ShowPartitionsCommand}
import org.apache.spark.sql.hive.{CatalogSchemaUtil, KatanaContext}
import org.apache.spark.sql.types.StringType

/**
  * @author angers.zhu@gmail.com
  * @date 2019/5/29 14:01
  */
case class KatanaShowPartitions(delegate: ShowPartitionsCommand)
                               (@transient private val katana: KatanaContext) extends RunnableCommand {
  override val output: Seq[Attribute] = {
    AttributeReference("partition", StringType, nullable = false)() :: Nil
  }

  override def run(sparkSession: SparkSession): Seq[Row] = {
    val catalog = CatalogSchemaUtil.getCatalog(delegate.tableName.catalog, sparkSession, katana)

    val table = catalog.getTableMetadata(delegate.tableName)
    val tableIdentWithDB = table.identifier.quotedString

    /**
      * Validate and throws an [[AnalysisException]] exception under the following conditions:
      * 1. If the table is not partitioned.
      * 2. If it is a datasource table.
      * 3. If it is a view.
      */
    if (table.tableType == VIEW) {
      throw new AnalysisException(s"SHOW PARTITIONS is not allowed on a view: $tableIdentWithDB")
    }

    if (table.partitionColumnNames.isEmpty) {
      throw new AnalysisException(
        s"SHOW PARTITIONS is not allowed on a table that is not partitioned: $tableIdentWithDB")
    }

    DDLUtils.verifyPartitionProviderIsHive(sparkSession, table, "SHOW PARTITIONS")

    /**
      * Validate the partitioning spec by making sure all the referenced columns are
      * defined as partitioning columns in table definition. An AnalysisException exception is
      * thrown if the partitioning spec is invalid.
      */
    if (delegate.spec.isDefined) {
      val badColumns = delegate.spec.get.keySet.filterNot(table.partitionColumnNames.contains)
      if (badColumns.nonEmpty) {
        val badCols = badColumns.mkString("[", ", ", "]")
        throw new AnalysisException(
          s"Non-partitioning column(s) $badCols are specified for SHOW PARTITIONS")
      }
    }

    val partNames = catalog.listPartitionNames(delegate.tableName, delegate.spec)
    partNames.map(Row(_))
  }
}
