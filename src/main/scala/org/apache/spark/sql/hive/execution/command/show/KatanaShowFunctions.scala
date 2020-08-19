package org.apache.spark.sql.hive.execution.command.show


import org.apache.spark.sql.{Row, SparkSession}
import org.apache.spark.sql.catalyst.expressions.Attribute
import org.apache.spark.sql.execution.command.{RunnableCommand, ShowFunctionsCommand}
import org.apache.spark.sql.hive.{CatalogSchemaUtil, KatanaContext}
import org.apache.spark.sql.types.{StringType, StructField, StructType}


case class KatanaShowFunctions(delegate: ShowFunctionsCommand)
                              (@transient private val katana: KatanaContext)
  extends RunnableCommand {

  override val output: Seq[Attribute] = {
    val schema = StructType(StructField("function", StringType, nullable = false) :: Nil)
    schema.toAttributes
  }

  override def run(sparkSession: SparkSession): Seq[Row] = {
    val catalog = CatalogSchemaUtil.getCatalog(delegate.catalog, sparkSession, katana)

    // when db is empty, catalog must be empty and will use current sessionState,
    // if current sessionState is none, use default.
    val dbName = delegate.db.getOrElse(
      katana.getActiveSession().getOrElse(
        sparkSession).sessionState.catalog.getCurrentDatabase)
    // If pattern is not specified, we use '*', which is used to
    // match any sequence of characters (including no characters).
    val functionNames =
    catalog
      .listFunctions(dbName, delegate.pattern.getOrElse("*"))
      .collect {
        case (f, "USER") if delegate.showUserFunctions => f.unquotedString
        case (f, "SYSTEM") if delegate.showSystemFunctions => f.unquotedString
      }
    functionNames.sorted.map(Row(_))
  }

}
