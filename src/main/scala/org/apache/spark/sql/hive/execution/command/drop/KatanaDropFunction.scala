package org.apache.spark.sql.hive.execution.command.drop

import org.apache.spark.sql.{AnalysisException, Row, SparkSession}
import org.apache.spark.sql.catalyst.{FunctionIdentifier, TableIdentifier}
import org.apache.spark.sql.catalyst.analysis.FunctionRegistry
import org.apache.spark.sql.catalyst.catalog.SessionCatalog
import org.apache.spark.sql.execution.command.{DropFunctionCommand, RunnableCommand}
import org.apache.spark.sql.hive.{KatanaContext, KatanaExtension, CatalogSchemaUtil}

import scala.collection.mutable.HashMap

/**
  * @author angers.zhu@gmail.com
  * @date 2019/5/29 14:20
  */
case class KatanaDropFunction(delegate: DropFunctionCommand,
                              hiveCatalogs: HashMap[String, SessionCatalog])
                             (@transient private val katana: KatanaContext) extends RunnableCommand {

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

    if (delegate.isTemp) {
      if (delegate.databaseName.isDefined) {
        throw new AnalysisException(s"Specifying a database in DROP TEMPORARY FUNCTION " +
          s"is not allowed: '${originDB}'")
      }
      if (FunctionRegistry.builtin.functionExists(FunctionIdentifier(delegate.functionName))) {
        throw new AnalysisException(s"Cannot drop native function '${delegate.functionName}'")
      }
      catalog.dropTempFunction(delegate.functionName, delegate.ifExists)
    } else {
      // We are dropping a permanent function.
      catalog.dropFunction(
        FunctionIdentifier(delegate.functionName, Some(originDB)),
        ignoreIfNotExists = delegate.ifExists)
    }
    Seq.empty[Row]
  }
}