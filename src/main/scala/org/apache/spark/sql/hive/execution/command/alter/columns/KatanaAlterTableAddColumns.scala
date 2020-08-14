package org.apache.spark.sql.hive.execution.command.alter.columns

import org.apache.spark.sql.catalyst.TableIdentifier
import org.apache.spark.sql.catalyst.catalog.{CatalogTable, CatalogTableType, SessionCatalog}
import org.apache.spark.sql.execution.command.{AlterTableAddColumnsCommand, DDLUtils, RunnableCommand}
import org.apache.spark.sql.execution.datasources.DataSource
import org.apache.spark.sql.execution.datasources.csv.CSVFileFormat
import org.apache.spark.sql.execution.datasources.json.JsonFileFormat
import org.apache.spark.sql.execution.datasources.parquet.ParquetFileFormat
import org.apache.spark.sql.hive.{KatanaContext, CatalogSchemaUtil}
import org.apache.spark.sql.internal.{SQLConf, SessionState}
import org.apache.spark.sql.types.StructType
import org.apache.spark.sql.util.SchemaUtils
import org.apache.spark.sql.{AnalysisException, Row, SparkSession}

import scala.collection.mutable.HashMap
import scala.util.control.NonFatal

/**
  * @author angers.zhu@gmail.com
  * @date 2019/5/30 18:33
  */
case class KatanaAlterTableAddColumns(delegate: AlterTableAddColumnsCommand,
                                      hiveCatalogs: HashMap[String, SessionCatalog])
                                     (@transient private val sessionState: SessionState,
                                      @transient private val katana: KatanaContext) extends RunnableCommand {
  override def run(sparkSession: SparkSession): Seq[Row] = {
    val catalog =
      CatalogSchemaUtil.getCatalog(
        delegate.table.catalog,
        hiveCatalogs,
        sparkSession,
        katana)

    val catalogTable = verifyAlterTableAddColumn(sessionState.conf, catalog, delegate.table)

    try {
      sparkSession.catalog.uncacheTable(delegate.table.quotedString)
    } catch {
      case NonFatal(e) =>
        log.warn(s"Exception when attempting to uncache table ${delegate.table.quotedString}", e)
    }
    catalog.refreshTable(delegate.table)

    SchemaUtils.checkColumnNameDuplication(
      (delegate.colsToAdd ++ catalogTable.schema).map(_.name),
      "in the table definition of " + delegate.table.identifier,
      conf.caseSensitiveAnalysis)
    DDLUtils.checkDataColNames(catalogTable, delegate.colsToAdd.map(_.name))

    catalog.alterTableDataSchema(delegate.table, StructType(catalogTable.dataSchema ++ delegate.colsToAdd))
    Seq.empty[Row]
  }

  /**
    * ALTER TABLE ADD COLUMNS command does not support temporary view/table,
    * view, or datasource table with text, orc formats or external provider.
    * For datasource table, it currently only supports parquet, json, csv.
    */
  private def verifyAlterTableAddColumn(conf: SQLConf,
                                        catalog: SessionCatalog,
                                        table: TableIdentifier): CatalogTable = {
    val catalogTable = catalog.getTempViewOrPermanentTableMetadata(table)

    if (catalogTable.tableType == CatalogTableType.VIEW) {
      throw new AnalysisException(
        s"""
           |ALTER ADD COLUMNS does not support views.
           |You must drop and re-create the views for adding the new columns. Views: $table
         """.stripMargin)
    }

    if (DDLUtils.isDatasourceTable(catalogTable)) {
      DataSource.lookupDataSource(catalogTable.provider.get, conf).newInstance() match {
        // For datasource table, this command can only support the following File format.
        // TextFileFormat only default to one column "value"
        // Hive type is already considered as hive serde table, so the logic will not
        // come in here.
        case _: JsonFileFormat | _: CSVFileFormat | _: ParquetFileFormat =>
        case s if s.getClass.getCanonicalName.endsWith("OrcFileFormat") =>
        case s =>
          throw new AnalysisException(
            s"""
               |ALTER ADD COLUMNS does not support datasource table with type $s.
               |You must drop and re-create the table for adding the new columns. Tables: $table
             """.stripMargin)
      }
    }
    catalogTable
  }
}
