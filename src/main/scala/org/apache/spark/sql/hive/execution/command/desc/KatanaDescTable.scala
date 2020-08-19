package org.apache.spark.sql.hive.execution.command.desc

import org.apache.spark.sql.{AnalysisException, Row, SparkSession}
import org.apache.spark.sql.catalyst.TableIdentifier
import org.apache.spark.sql.catalyst.catalog.{CatalogTable, CatalogTablePartition, CatalogTableType, SessionCatalog}
import org.apache.spark.sql.catalyst.expressions.{Attribute, AttributeReference}
import org.apache.spark.sql.execution.command.{DDLUtils, DescribeTableCommand, RunnableCommand}
import org.apache.spark.sql.hive.{CatalogSchemaUtil, KatanaContext}
import org.apache.spark.sql.types.{MetadataBuilder, StringType, StructType}

import scala.collection.mutable.ArrayBuffer

/**
  * @author angers.zhu@gmail.com
  * @date 2019/5/29 10:47
  */
case class KatanaDescTable(delegate: DescribeTableCommand)
                          (@transient private val katana: KatanaContext) extends RunnableCommand {
  override val output: Seq[Attribute] = Seq(
    // Column names are based on Hive.
    AttributeReference("col_name", StringType, nullable = false,
      new MetadataBuilder().putString("comment", "name of the column").build())(),
    AttributeReference("data_type", StringType, nullable = false,
      new MetadataBuilder().putString("comment", "data type of the column").build())(),
    AttributeReference("comment", StringType, nullable = true,
      new MetadataBuilder().putString("comment", "comment of the column").build())()
  )


  override def run(sparkSession: SparkSession): Seq[Row] = {
    val result = new ArrayBuffer[Row]
    val catalog = CatalogSchemaUtil.getCatalog(delegate.table.catalog, sparkSession, katana)

    if (catalog.isTemporaryTable(delegate.table)) {
      if (delegate.partitionSpec.nonEmpty) {
        throw new AnalysisException(
          s"DESC PARTITION is not allowed on a temporary view: ${delegate.table.identifier}")
      }
      describeSchema(catalog.lookupRelation(delegate.table).schema, result, header = false)
    } else {
      val metadata = catalog.getTableMetadata(delegate.table)
      if (metadata.schema.isEmpty) {
        // In older version(prior to 2.1) of Spark, the table schema can be empty and should be
        // inferred at runtime. We should still support it.
        describeSchema(sparkSession.table(metadata.identifier).schema, result, header = false)
      } else {
        describeSchema(metadata.schema, result, header = false)
      }

      describePartitionInfo(metadata, result)

      if (delegate.partitionSpec.nonEmpty) {
        // Outputs the partition-specific info for the DDL command:
        // "DESCRIBE [EXTENDED|FORMATTED] table_name PARTITION (partitionVal*)"
        describeDetailedPartitionInfo(delegate.table, sparkSession, catalog, metadata, result)
      } else if (delegate.isExtended) {
        describeFormattedTableInfo(metadata, result)
      }
    }

    result
  }

  private def describePartitionInfo(table: CatalogTable, buffer: ArrayBuffer[Row]): Unit = {
    if (table.partitionColumnNames.nonEmpty) {
      append(buffer, "# Partition Information", "", "")
      describeSchema(table.partitionSchema, buffer, header = true)
    }
  }

  private def describeFormattedTableInfo(table: CatalogTable, buffer: ArrayBuffer[Row]): Unit = {
    // The following information has been already shown in the previous outputs
    val excludedTableInfo = Seq(
      "Partition Columns",
      "Schema"
    )
    append(buffer, "", "", "")
    append(buffer, "# Detailed Table Information", "", "")
    table.toLinkedHashMap.filterKeys(!excludedTableInfo.contains(_)).foreach {
      s => append(buffer, s._1, s._2, "")
    }
  }

  private def describeDetailedPartitionInfo(originTableIdentifier: TableIdentifier,
                                            spark: SparkSession,
                                            catalog: SessionCatalog,
                                            metadata: CatalogTable,
                                            result: ArrayBuffer[Row]): Unit = {
    if (metadata.tableType == CatalogTableType.VIEW) {
      throw new AnalysisException(
        s"DESC PARTITION is not allowed on a view: ${originTableIdentifier.identifier}")
    }
    DDLUtils.verifyPartitionProviderIsHive(spark, metadata, "DESC PARTITION")
    val partition = catalog.getPartition(originTableIdentifier, delegate.partitionSpec)
    if (delegate.isExtended) describeFormattedDetailedPartitionInfo(originTableIdentifier, metadata, partition, result)
  }

  private def describeFormattedDetailedPartitionInfo(tableIdentifier: TableIdentifier,
                                                     table: CatalogTable,
                                                     partition: CatalogTablePartition,
                                                     buffer: ArrayBuffer[Row]): Unit = {
    append(buffer, "", "", "")
    append(buffer, "# Detailed Partition Information", "", "")
    append(buffer, "Database", table.database, "")
    append(buffer, "Table", tableIdentifier.table, "")
    partition.toLinkedHashMap.foreach(s => append(buffer, s._1, s._2, ""))
    append(buffer, "", "", "")
    append(buffer, "# Storage Information", "", "")
    table.bucketSpec match {
      case Some(spec) =>
        spec.toLinkedHashMap.foreach(s => append(buffer, s._1, s._2, ""))
      case _ =>
    }
    table.storage.toLinkedHashMap.foreach(s => append(buffer, s._1, s._2, ""))
  }

  private def describeSchema(schema: StructType,
                             buffer: ArrayBuffer[Row],
                             header: Boolean): Unit = {
    if (header) {
      append(buffer, s"# ${output.head.name}", output(1).name, output(2).name)
    }
    schema.foreach { column =>
      append(buffer, column.name, column.dataType.simpleString, column.getComment().orNull)
    }
  }

  private def append(buffer: ArrayBuffer[Row], column: String, dataType: String, comment: String): Unit = {
    buffer += Row(column, dataType, comment)
  }
}
