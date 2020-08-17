package org.apache.spark.sql.hive.execution.command.create

import org.apache.spark.sql.{AnalysisException, Row, SparkSession}
import org.apache.spark.sql.catalyst.FunctionIdentifier
import org.apache.spark.sql.catalyst.catalog.CatalogFunction
import org.apache.spark.sql.execution.command.{CreateFunctionCommand, RunnableCommand}
import org.apache.spark.sql.hive.{CatalogSchemaUtil, KatanaContext}

/**
  * @author angers.zhu@gmail.com
  * @date 2019/5/29 15:02
  */
case class KatanaCreateFunction(delegate: CreateFunctionCommand)
                               (@transient private val katana: KatanaContext) extends RunnableCommand {

  if (delegate.ignoreIfExists && delegate.replace) {
    throw new AnalysisException("CREATE FUNCTION with both IF NOT EXISTS and REPLACE" +
      " is not allowed.")
  }

  // Disallow to define a temporary function with `IF NOT EXISTS`
  if (delegate.ignoreIfExists && delegate.isTemp) {
    throw new AnalysisException(
      "It is not allowed to define a TEMPORARY function with IF NOT EXISTS.")
  }

  // Temporary function names should not contain database prefix like "database.function"
  if (delegate.databaseName.isDefined && delegate.isTemp) {
    throw new AnalysisException(s"Specifying a database in CREATE TEMPORARY FUNCTION " +
      s"is not allowed: '${delegate.databaseName.get}'")
  }

  override def run(sparkSession: SparkSession): Seq[Row] = {
    val catalog =
      CatalogSchemaUtil.getCatalog(
        delegate.catalog,
        sparkSession,
        katana)

    val func = CatalogFunction(FunctionIdentifier(delegate.functionName, delegate.databaseName), delegate.className, delegate.resources)
    if (delegate.isTemp) {
      // We first load resources and then put the builder in the function registry.
      catalog.loadFunctionResources(delegate.resources)
      catalog.registerFunction(func, overrideIfExists = delegate.replace)
    } else {
      // Handles `CREATE OR REPLACE FUNCTION AS ... USING ...`
      if (delegate.replace && catalog.functionExists(func.identifier)) {
        // alter the function in the metastore
        catalog.alterFunction(func)
      } else {
        // For a permanent, we will store the metadata into underlying external catalog.
        // This function will be loaded into the FunctionRegistry when a query uses it.
        // We do not load it into FunctionRegistry right now.
        catalog.createFunction(func, delegate.ignoreIfExists)
      }
    }
    Seq.empty[Row]
  }
}
