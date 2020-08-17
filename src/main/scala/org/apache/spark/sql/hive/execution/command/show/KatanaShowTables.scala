package org.apache.spark.sql.hive.execution.command.show

import org.apache.spark.sql.{Row, SparkSession}
import org.apache.spark.sql.catalyst.TableIdentifier
import org.apache.spark.sql.catalyst.expressions.{Attribute, AttributeReference}
import org.apache.spark.sql.execution.command.{RunnableCommand, ShowTablesCommand}
import org.apache.spark.sql.hive.{CatalogSchemaUtil, KatanaContext}
import org.apache.spark.sql.types.{BooleanType, StringType}

/**
 * @author angers.zhu@gmail.com
 * @date 2019/5/28 18:13
 */
case class KatanaShowTables(delegate: ShowTablesCommand)
                           (@transient private val katana: KatanaContext) extends RunnableCommand {
  override val output: Seq[Attribute] = {
    val tableExtendedInfo = if (delegate.isExtended) {
      AttributeReference("information", StringType, nullable = false)() :: Nil
    } else {
      Nil
    }
    AttributeReference("catalog", StringType, nullable = false)() ::
      AttributeReference("database", StringType, nullable = false)() ::
      AttributeReference("tableName", StringType, nullable = false)() ::
      AttributeReference("isTemporary", BooleanType, nullable = false)() :: tableExtendedInfo
  }

  override def run(sparkSession: SparkSession): Seq[Row] = {
    val catalog = CatalogSchemaUtil.getCatalog(delegate.catalog, sparkSession, katana)
    val catalogName = CatalogSchemaUtil.getCatalogName(catalog, katana)
    val db = delegate.databaseName.getOrElse(catalog.getCurrentDatabase)
    if (delegate.partitionSpec.isEmpty) {
      // Show the information of tables.
      val tables =
        delegate.tableIdentifierPattern.map(catalog.listTables(db, _))
          .getOrElse(catalog.listTables(db))
      tables.map { tableIdent =>
        val database = tableIdent.database.getOrElse("")
        val tableName = tableIdent.table
        val isTemp = catalog.isTemporaryTable(tableIdent)
        if (delegate.isExtended) {
          val information = catalog.getTempViewOrPermanentTableMetadata(tableIdent).simpleString
          Row(catalogName.orNull, database, tableName, isTemp, s"$information\n")
        } else {
          Row(catalogName.orNull, database, tableName, isTemp)
        }
      }
    } else {
      // Show the information of partitions.
      //
      // Note: tableIdentifierPattern should be non-empty, otherwise a [[ParseException]]
      // should have been thrown by the sql parser.
      val tableIdent = TableIdentifier(delegate.tableIdentifierPattern.get, Some(db), delegate.catalog)
      val table = catalog.getTableMetadata(tableIdent).identifier
      val partition = catalog.getPartition(tableIdent, delegate.partitionSpec.get)
      val database = table.database.getOrElse("")
      val tableName = table.table
      val isTemp = catalog.isTemporaryTable(table)
      val information = partition.simpleString
      Seq(Row(catalogName.orNull, database, tableName, isTemp, s"$information\n"))
    }
  }
}
