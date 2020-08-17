package org.apache.spark.sql.hive.execution.command.desc

import org.apache.spark.sql.catalyst.catalog.{CatalogDatabase, CatalogUtils, SessionCatalog}
import org.apache.spark.sql.catalyst.expressions.{Attribute, AttributeReference}
import org.apache.spark.sql.execution.command.{DescribeDatabaseCommand, RunnableCommand}
import org.apache.spark.sql.hive.{CatalogSchemaUtil, KatanaContext}
import org.apache.spark.sql.types.StringType
import org.apache.spark.sql.{Row, SparkSession}

import scala.collection.mutable.HashMap

/**
  * @author angers.zhu@gmail.com
  * @date 2019/5/29 10:42
  */
case class KatanaDescDatabase(delegate: DescribeDatabaseCommand,
                              hiveCatalogs: HashMap[String, SessionCatalog])
                             (@transient private val katana: KatanaContext)extends RunnableCommand {

  override val output: Seq[Attribute] = {
    AttributeReference("database_description_item", StringType, nullable = false)() ::
      AttributeReference("database_description_value", StringType, nullable = false)() :: Nil
  }

  override def run(sparkSession: SparkSession): Seq[Row] = {
    val catalog = CatalogSchemaUtil.getCatalog(delegate.catalog, hiveCatalogs, sparkSession, katana)
    val catalogName = CatalogSchemaUtil.getCatalogName(catalog, hiveCatalogs)
    val dbMetadata: CatalogDatabase =
      catalog.getDatabaseMetadata(delegate.databaseName)
    val result =
      Row("Catalog", catalogName) ::
        Row("Database Name", dbMetadata.name) ::
        Row("Description", dbMetadata.description) ::
        Row("Location", CatalogUtils.URIToString(dbMetadata.locationUri)) :: Nil

    if (delegate.extended) {
      val properties =
        if (dbMetadata.properties.isEmpty) {
          ""
        } else {
          dbMetadata.properties.toSeq.mkString("(", ", ", ")")
        }
      result :+ Row("Properties", properties)
    } else {
      result
    }
  }


}
