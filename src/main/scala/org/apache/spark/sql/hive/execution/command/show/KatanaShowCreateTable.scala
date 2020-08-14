package org.apache.spark.sql.hive.execution.command.show

import org.apache.spark.sql.{AnalysisException, Row, SparkSession}
import org.apache.spark.sql.catalyst.catalog.CatalogTableType.{EXTERNAL, MANAGED, VIEW}
import org.apache.spark.sql.catalyst.catalog.{CatalogTable, CatalogUtils, SessionCatalog}
import org.apache.spark.sql.catalyst.expressions.{Attribute, AttributeReference}
import org.apache.spark.sql.catalyst.util.{escapeSingleQuotedString, quoteIdentifier}
import org.apache.spark.sql.execution.command.{DDLUtils, RunnableCommand, ShowCreateTableCommand}
import org.apache.spark.sql.hive.{KatanaContext, KatanaExtension, CatalogSchemaUtil}
import org.apache.spark.sql.types.StringType

import scala.collection.mutable

/**
  * @author angers.zhu@gmail.com
  * @date 2019/5/29 11:08
  */
case class KatanaShowCreateTable(delegate: ShowCreateTableCommand,
                                 hiveCatalogs: mutable.HashMap[String, SessionCatalog])
                                (@transient private val katana: KatanaContext) extends RunnableCommand {
  override val output: Seq[Attribute] = Seq(
    AttributeReference("createtab_stmt", StringType, nullable = false)()
  )

  override def run(sparkSession: SparkSession): Seq[Row] = {
    val catalog =
      CatalogSchemaUtil.getCatalog(
        delegate.table.catalog,
        hiveCatalogs,
        sparkSession,
        katana)

    val tableMetadata = catalog.getTableMetadata(delegate.table)

    // TODO: unify this after we unify the CREATE TABLE syntax for hive serde and data source table.
    val stmt = if (DDLUtils.isDatasourceTable(tableMetadata)) {
      showCreateDataSourceTable(tableMetadata)
    } else {
      showCreateHiveTable(tableMetadata)
    }

    Seq(Row(stmt))
  }

  private def showCreateHiveTable(metadata: CatalogTable): String = {
    def reportUnsupportedError(features: Seq[String]): Unit = {
      throw new AnalysisException(
        s"Failed to execute SHOW CREATE TABLE against table/view ${metadata.identifier}, " +
          "which is created by Hive and uses the following unsupported feature(s)\n" +
          features.map(" - " + _).mkString("\n")
      )
    }

    if (metadata.unsupportedFeatures.nonEmpty) {
      reportUnsupportedError(metadata.unsupportedFeatures)
    }

    val builder = StringBuilder.newBuilder

    val tableTypeString = metadata.tableType match {
      case EXTERNAL => " EXTERNAL TABLE"
      case VIEW => " VIEW"
      case MANAGED => " TABLE"
      case t =>
        throw new IllegalArgumentException(
          s"Unknown table type is found at showCreateHiveTable: $t")
    }

    builder ++= s"CREATE$tableTypeString ${delegate.table.quotedString}"

    if (metadata.tableType == VIEW) {
      if (metadata.schema.nonEmpty) {
        builder ++= metadata.schema.map(_.name).mkString("(", ", ", ")")
      }
      builder ++= metadata.viewText.mkString(" AS\n", "", "\n")
    } else {
      showHiveTableHeader(metadata, builder)
      showHiveTableNonDataColumns(metadata, builder)
      showHiveTableStorageInfo(metadata, builder)
      showHiveTableProperties(metadata, builder)
    }

    builder.toString()
  }

  private def showHiveTableHeader(metadata: CatalogTable, builder: StringBuilder): Unit = {
    val columns = metadata.schema.filterNot { column =>
      metadata.partitionColumnNames.contains(column.name)
    }.map(_.toDDL)

    if (columns.nonEmpty) {
      builder ++= columns.mkString("(", ", ", ")\n")
    }

    metadata
      .comment
      .map("COMMENT '" + escapeSingleQuotedString(_) + "'\n")
      .foreach(builder.append)
  }


  private def showHiveTableNonDataColumns(metadata: CatalogTable, builder: StringBuilder): Unit = {
    if (metadata.partitionColumnNames.nonEmpty) {
      val partCols = metadata.partitionSchema.map(_.toDDL)
      builder ++= partCols.mkString("PARTITIONED BY (", ", ", ")\n")
    }

    if (metadata.bucketSpec.isDefined) {
      val bucketSpec = metadata.bucketSpec.get
      builder ++= s"CLUSTERED BY (${bucketSpec.bucketColumnNames.mkString(",")})\n"

      if (bucketSpec.sortColumnNames.nonEmpty) {
        builder ++= s"SORTED BY (${bucketSpec.sortColumnNames.map(_ + " ASC").mkString(", ")})\n"
      }
      builder ++= s"INTO ${bucketSpec.numBuckets} BUCKETS\n"
    }
  }

  private def showHiveTableStorageInfo(metadata: CatalogTable, builder: StringBuilder): Unit = {
    val storage = metadata.storage

    storage.serde.foreach { serde =>
      builder ++= s"ROW FORMAT SERDE '$serde'\n"

      val serdeProps = metadata.storage.properties.map {
        case (key, value) =>
          s"'${escapeSingleQuotedString(key)}' = '${escapeSingleQuotedString(value)}'"
      }

      builder ++= serdeProps.mkString("WITH SERDEPROPERTIES (\n  ", ",\n  ", "\n)\n")
    }

    if (storage.inputFormat.isDefined || storage.outputFormat.isDefined) {
      builder ++= "STORED AS\n"

      storage.inputFormat.foreach { format =>
        builder ++= s"  INPUTFORMAT '${escapeSingleQuotedString(format)}'\n"
      }

      storage.outputFormat.foreach { format =>
        builder ++= s"  OUTPUTFORMAT '${escapeSingleQuotedString(format)}'\n"
      }
    }

    if (metadata.tableType == EXTERNAL) {
      storage.locationUri.foreach { uri =>
        builder ++= s"LOCATION '$uri'\n"
      }
    }
  }

  private def showHiveTableProperties(metadata: CatalogTable, builder: StringBuilder): Unit = {
    if (metadata.properties.nonEmpty) {
      val props = metadata.properties.map { case (key, value) =>
        s"'${escapeSingleQuotedString(key)}' = '${escapeSingleQuotedString(value)}'"
      }

      builder ++= props.mkString("TBLPROPERTIES (\n  ", ",\n  ", "\n)\n")
    }
  }

  private def showCreateDataSourceTable(metadata: CatalogTable): String = {
    val builder = StringBuilder.newBuilder

    builder ++= s"CREATE TABLE ${delegate.table.quotedString} "
    showDataSourceTableDataColumns(metadata, builder)
    showDataSourceTableOptions(metadata, builder)
    showDataSourceTableNonDataColumns(metadata, builder)

    builder.toString()
  }

  private def showDataSourceTableDataColumns(metadata: CatalogTable, builder: StringBuilder): Unit = {
    val columns = metadata.schema.fields.map(_.toDDL)
    builder ++= columns.mkString("(", ", ", ")\n")
  }

  private def showDataSourceTableOptions(metadata: CatalogTable, builder: StringBuilder): Unit = {
    builder ++= s"USING ${metadata.provider.get}\n"

    val dataSourceOptions = metadata.storage.properties.map {
      case (key, value) => s"${quoteIdentifier(key)} '${escapeSingleQuotedString(value)}'"
    } ++ metadata.storage.locationUri.flatMap { location =>
      if (metadata.tableType == MANAGED) {
        // If it's a managed table, omit PATH option. Spark SQL always creates external table
        // when the table creation DDL contains the PATH option.
        None
      } else {
        Some(s"path '${escapeSingleQuotedString(CatalogUtils.URIToString(location))}'")
      }
    }

    if (dataSourceOptions.nonEmpty) {
      builder ++= "OPTIONS (\n"
      builder ++= dataSourceOptions.mkString("  ", ",\n  ", "\n")
      builder ++= ")\n"
    }
  }

  private def showDataSourceTableNonDataColumns(metadata: CatalogTable, builder: StringBuilder): Unit = {
    val partCols = metadata.partitionColumnNames
    if (partCols.nonEmpty) {
      builder ++= s"PARTITIONED BY ${partCols.mkString("(", ", ", ")")}\n"
    }

    metadata.bucketSpec.foreach { spec =>
      if (spec.bucketColumnNames.nonEmpty) {
        builder ++= s"CLUSTERED BY ${spec.bucketColumnNames.mkString("(", ", ", ")")}\n"

        if (spec.sortColumnNames.nonEmpty) {
          builder ++= s"SORTED BY ${spec.sortColumnNames.mkString("(", ", ", ")")}\n"
        }

        builder ++= s"INTO ${spec.numBuckets} BUCKETS\n"
      }
    }
  }
}
