package org.apache.spark.sql.hive.execution.command.alter.columns

import org.apache.spark.sql.catalyst.TableIdentifier
import org.apache.spark.sql.catalyst.analysis.Resolver
import org.apache.spark.sql.catalyst.catalog.SessionCatalog
import org.apache.spark.sql.execution.command.{AlterTableChangeColumnCommand, DDLUtils, RunnableCommand}
import org.apache.spark.sql.hive.{KatanaContext, CatalogSchemaUtil}
import org.apache.spark.sql.types.{StructField, StructType}
import org.apache.spark.sql.{AnalysisException, Row, SparkSession}

import scala.collection.mutable.HashMap

/**
  * @author angers.zhu@gmail.com
  * @date 2019/5/30 18:34
  */
case class KatanaAlterTableChangeColumn(delegate: AlterTableChangeColumnCommand,
                                        hiveCatalogs: HashMap[String, SessionCatalog])
                                       (@transient private val katana: KatanaContext) extends RunnableCommand {

  // TODO: support change column name/dataType/metadata/position.
  override def run(sparkSession: SparkSession): Seq[Row] = {
    val (catalog: SessionCatalog, originDB: String) = delegate.tableName.database match {
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

    val originTableIdentifier = new TableIdentifier(delegate.tableName.table, Some(originDB))
    val table = catalog.getTableMetadata(originTableIdentifier)
    val resolver = sparkSession.sessionState.conf.resolver
    DDLUtils.verifyAlterTableType(catalog, table, isView = false)

    // Find the origin column from dataSchema by column name.
    val originColumn = findColumnByName(table.dataSchema, delegate.columnName, resolver)
    // Throw an AnalysisException if the column name/dataType is changed.
    if (!columnEqual(originColumn, delegate.newColumn, resolver)) {
      throw new AnalysisException(
        "ALTER TABLE CHANGE COLUMN is not supported for changing column " +
          s"'${originColumn.name}' with type '${originColumn.dataType}' to " +
          s"'${delegate.newColumn.name}' with type '${delegate.newColumn.dataType}'")
    }

    val newDataSchema = table.dataSchema.fields.map { field =>
      if (field.name == originColumn.name) {
        // Create a new column from the origin column with the new comment.
        addComment(field, delegate.newColumn.getComment)
      } else {
        field
      }
    }
    catalog.alterTableDataSchema(originTableIdentifier, StructType(newDataSchema))

    Seq.empty[Row]
  }

  // Find the origin column from schema by column name, throw an AnalysisException if the column
  // reference is invalid.
  private def findColumnByName(schema: StructType, name: String, resolver: Resolver): StructField = {
    schema.fields.collectFirst {
      case field if resolver(field.name, name) => field
    }.getOrElse(throw new AnalysisException(
      s"Can't find column `$name` given table data columns " +
        s"${schema.fieldNames.mkString("[`", "`, `", "`]")}"))
  }

  // Add the comment to a column, if comment is empty, return the original column.
  private def addComment(column: StructField, comment: Option[String]): StructField = {
    comment.map(column.withComment(_)).getOrElse(column)
  }

  // Compare a [[StructField]] to another, return true if they have the same column
  // name(by resolver) and dataType.
  private def columnEqual(field: StructField, other: StructField, resolver: Resolver): Boolean = {
    resolver(field.name, other.name) && field.dataType == other.dataType
  }
}
