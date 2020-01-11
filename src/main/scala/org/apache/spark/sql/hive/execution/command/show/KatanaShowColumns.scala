package org.apache.spark.sql.hive.execution.command.show

import org.apache.spark.sql.catalyst.TableIdentifier
import org.apache.spark.sql.catalyst.catalog.SessionCatalog
import org.apache.spark.sql.catalyst.expressions.{Attribute, AttributeReference}
import org.apache.spark.sql.execution.command.{RunnableCommand, ShowColumnsCommand}
import org.apache.spark.sql.hive.{KatanaContext, CatalogSchemaUtil}
import org.apache.spark.sql.types.StringType
import org.apache.spark.sql.{AnalysisException, Row, SparkSession}

import scala.collection.mutable.HashMap

/**
  * @author angers.zhu@gmail.com
  * @date 2019/5/29 10:12
  */
case class KatanaShowColumns(delegate: ShowColumnsCommand,
                             hiveCatalogs: HashMap[String, SessionCatalog])
                            (@transient private val katana: KatanaContext) extends RunnableCommand {
  override val output: Seq[Attribute] = {
    AttributeReference("col_name", StringType, nullable = false)() :: Nil
  }

  override def run(sparkSession: SparkSession): Seq[Row] = {

    val resolver = sparkSession.sessionState.conf.resolver
    val (catalog: SessionCatalog, originDB: String) = delegate.databaseName match {
      case None => {
        if (delegate.tableName.database.isEmpty) {
          val tempCatalog =
            if (katana.getActiveSessionState() == null)
              sparkSession.sessionState.catalog
            else
              katana.getActiveSessionState().catalog
          (tempCatalog, tempCatalog.getCurrentDatabase)
        } else {
          CatalogSchemaUtil.getCatalogAndOriginDBName(hiveCatalogs, delegate.tableName.database.get)(sparkSession)
        }
      }

      case Some(db) if delegate.tableName.database.exists(!resolver(_, db)) =>
        throw new AnalysisException(
          s"SHOW COLUMNS with conflicting databases: '$db' != '${delegate.tableName.database.get}'")

      case Some(db) => CatalogSchemaUtil.getCatalogAndOriginDBName(hiveCatalogs, db)(sparkSession)
    }


    val lookupTable = TableIdentifier(delegate.tableName.identifier, Some(originDB))

    val table = catalog.getTempViewOrPermanentTableMetadata(lookupTable)
    table.schema.map { c =>
      Row(c.name)
    }
  }
}
