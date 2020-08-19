package org.apache.spark.sql.hive.execution.command.drop

import org.apache.spark.sql.{AnalysisException, Row, SparkSession}
import org.apache.spark.sql.catalyst.FunctionIdentifier
import org.apache.spark.sql.catalyst.analysis.FunctionRegistry
import org.apache.spark.sql.execution.command.{DropFunctionCommand, RunnableCommand}
import org.apache.spark.sql.hive.{CatalogSchemaUtil, KatanaContext}

/**
  * @author angers.zhu@gmail.com
  * @date 2019/5/29 14:20
  */
case class KatanaDropFunction(
    delegate: DropFunctionCommand)
    (@transient private val katana: KatanaContext)
  extends RunnableCommand {

  override def run(sparkSession: SparkSession): Seq[Row] = {
    val catalog = CatalogSchemaUtil.getCatalog(delegate.catalog, sparkSession, katana)

    if (delegate.isTemp) {
      if (delegate.databaseName.isDefined) {
        throw new AnalysisException(s"Specifying a database in DROP TEMPORARY FUNCTION " +
          s"is not allowed: '${delegate.databaseName}'")
      }
      if (FunctionRegistry.builtin.functionExists(FunctionIdentifier(delegate.functionName))) {
        throw new AnalysisException(s"Cannot drop native function '${delegate.functionName}'")
      }
      catalog.dropTempFunction(delegate.functionName, delegate.ifExists)
    } else {
      // We are dropping a permanent function.
      catalog.dropFunction(
        FunctionIdentifier(delegate.functionName, delegate.databaseName),
        ignoreIfNotExists = delegate.ifExists)
    }
    Seq.empty[Row]
  }
}
