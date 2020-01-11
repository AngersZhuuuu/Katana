package org.apache.spark.sql.hive.execution.command.show

import org.apache.spark.sql.catalyst.TableIdentifier
import org.apache.spark.sql.catalyst.catalog.SessionCatalog
import org.apache.spark.sql.catalyst.expressions.{Attribute, AttributeReference}
import org.apache.spark.sql.execution.command.{RunnableCommand, ShowTablePropertiesCommand}
import org.apache.spark.sql.hive.{KatanaContext, CatalogSchemaUtil}
import org.apache.spark.sql.types.StringType
import org.apache.spark.sql.{Row, SparkSession}

import scala.collection.mutable.HashMap

/**
  * @author angers.zhu@gmail.com
  * @date 2019/5/29 14:08
  */
case class KatanaShowTableProperties(delegate: ShowTablePropertiesCommand,
                                     hiveCatalogs: HashMap[String, SessionCatalog])
                                    (@transient private val katana: KatanaContext) extends RunnableCommand {

  override val output: Seq[Attribute] = {
    val schema = AttributeReference("value", StringType, nullable = false)() :: Nil
    delegate.propertyKey match {
      case None => AttributeReference("key", StringType, nullable = false)() :: schema
      case _ => schema
    }
  }

  override def run(sparkSession: SparkSession): Seq[Row] = {
    val (catalog: SessionCatalog, originDB: String) = delegate.table.database match {
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
    val originTableIdentifier = new TableIdentifier(table = delegate.table.table, database = Some(originDB))

    if (catalog.isTemporaryTable(originTableIdentifier)) {
      Seq.empty[Row]
    } else {
      val catalogTable = catalog.getTableMetadata(originTableIdentifier)

      delegate.propertyKey match {
        case Some(p) =>
          val propValue = catalogTable
            .properties
            .getOrElse(p, s"Table ${catalogTable.qualifiedName} does not have property: $p")
          Seq(Row(propValue))
        case None =>
          catalogTable.properties.map(p => Row(p._1, p._2)).toSeq
      }
    }
  }
}
