package org.apache.spark.sql.hive.execution.command.show

import org.apache.spark.sql.catalyst.TableIdentifier
import org.apache.spark.sql.catalyst.catalog.SessionCatalog
import org.apache.spark.sql.catalyst.expressions.{Attribute, AttributeReference}
import org.apache.spark.sql.execution.command.{RunnableCommand, ShowTablesCommand}
import org.apache.spark.sql.hive.{KatanaContext, KatanaExtension, CatalogSchemaUtil}
import org.apache.spark.sql.types.{BooleanType, StringType}
import org.apache.spark.sql.{AnalysisException, Row, SparkSession}

import scala.collection.mutable.HashMap

/**
  * @author angers.zhu@gmail.com
  * @date 2019/5/28 18:13
  */
case class KatanaShowTables(delegate: ShowTablesCommand,
                            hiveCatalogs: HashMap[String, SessionCatalog])
                           (@transient private val katana: KatanaContext) extends RunnableCommand {
  override val output: Seq[Attribute] = {
    val tableExtendedInfo = if (delegate.isExtended) {
      AttributeReference("information", StringType, nullable = false)() :: Nil
    } else {
      Nil
    }
    AttributeReference("database", StringType, nullable = false)() ::
      AttributeReference("tableName", StringType, nullable = false)() ::
      AttributeReference("isTemporary", BooleanType, nullable = false)() :: tableExtendedInfo
  }

  override def run(sparkSession: SparkSession): Seq[Row] = {
    val (catalog: SessionCatalog, originDB: String) = delegate.databaseName match {
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

    /**
      * 带有Schema的DB 用于在SparkSession 生成 UnresolvedRelation 进行路由
      */
    val hiveSchema = hiveCatalogs.find(_._2 == catalog)

    if (delegate.partitionSpec.isEmpty) {
      // Show the information of tables.
      val tables =
        delegate.tableIdentifierPattern.map(catalog.listTables(originDB, _)).getOrElse(catalog.listTables(originDB))
      tables.map { tableIdent =>
        val database = tableIdent.database.getOrElse("")
        val tableName = tableIdent.table
        val isTemp = catalog.isTemporaryTable(tableIdent)
        if (delegate.isExtended) {
          val information = catalog.getTempViewOrPermanentTableMetadata(tableIdent).simpleString
          Row(if (hiveSchema.isDefined) hiveSchema.get._1 + "_" + database else database, tableName, isTemp, s"$information\n")
        } else {
          Row(if (hiveSchema.isDefined) hiveSchema.get._1 + "_" + database else database, tableName, isTemp)
        }
      }
    } else {
      // Show the information of partitions.
      //
      // Note: tableIdentifierPattern should be non-empty, otherwise a [[ParseException]]
      // should have been thrown by the sql parser.
      val tableIdent = TableIdentifier(delegate.tableIdentifierPattern.get, Some(originDB))
      val table = catalog.getTableMetadata(tableIdent).identifier
      val partition = catalog.getPartition(tableIdent, delegate.partitionSpec.get)
      val database = table.database.getOrElse("")
      val tableName = table.table
      val isTemp = catalog.isTemporaryTable(table)
      val information = partition.simpleString
      Seq(Row(database, tableName, isTemp, s"$information\n"))
    }
  }
}
