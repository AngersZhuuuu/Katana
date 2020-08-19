package org.apache.spark.sql.hive.execution.command.desc

import java.util.Locale

import org.apache.spark.sql.{Row, SparkSession}
import org.apache.spark.sql.catalyst.analysis.NoSuchFunctionException
import org.apache.spark.sql.catalyst.expressions.Attribute
import org.apache.spark.sql.execution.command.{DescribeFunctionCommand, RunnableCommand}
import org.apache.spark.sql.hive.{CatalogSchemaUtil, KatanaContext}
import org.apache.spark.sql.types.{StringType, StructField, StructType}

/**
  * @author angers.zhu@gmail.com
  * @date 2019/5/29 11:01
  */
case class KatanaDescFunction(
    delegate: DescribeFunctionCommand)
    (@transient private val katana: KatanaContext)
  extends RunnableCommand {

  override val output: Seq[Attribute] = {
    val schema = StructType(StructField("function_desc", StringType, nullable = false) :: Nil)
    schema.toAttributes
  }

  private def replaceFunctionName(usage: String, functionName: String): String = {
    if (usage == null) {
      "N/A."
    } else {
      usage.replaceAll("_FUNC_", functionName)
    }
  }

  override def run(sparkSession: SparkSession): Seq[Row] = {
    // Hard code "<>", "!=", "between", and "case" for now as there is no corresponding functions.
    val catalog = CatalogSchemaUtil.getCatalog(delegate.functionName.catalog, sparkSession, katana)

    delegate.functionName.funcName.toLowerCase(Locale.ROOT) match {
      case "<>" =>
        Row(s"Function: ${delegate.functionName}") ::
          Row("Usage: expr1 <> expr2 - " +
            "Returns true if `expr1` is not equal to `expr2`.") :: Nil
      case "!=" =>
        Row(s"Function: ${delegate.functionName}") ::
          Row("Usage: expr1 != expr2 - " +
            "Returns true if `expr1` is not equal to `expr2`.") :: Nil
      case "between" =>
        Row("Function: between") ::
          Row("Usage: expr1 [NOT] BETWEEN expr2 AND expr3 - " +
            "evaluate if `expr1` is [not] in between `expr2` and `expr3`.") :: Nil
      case "case" =>
        Row("Function: case") ::
          Row("Usage: CASE expr1 WHEN expr2 THEN expr3 " +
            "[WHEN expr4 THEN expr5]* [ELSE expr6] END - " +
            "When `expr1` = `expr2`, returns `expr3`; " +
            "when `expr1` = `expr4`, return `expr5`; else return `expr6`.") :: Nil
      case _ =>
        try {
          val info = catalog.lookupFunctionInfo(delegate.functionName)
          val name = if (info.getDb != null) info.getDb + "." + info.getName else info.getName
          val result =
            Row(s"Function: $name") ::
              Row(s"Class: ${info.getClassName}") ::
              Row(s"Usage: ${replaceFunctionName(info.getUsage, info.getName)}") :: Nil

          if (delegate.isExtended) {
            result :+
              Row(s"Extended Usage:${replaceFunctionName(info.getExtended, info.getName)}")
          } else {
            result
          }
        } catch {
          case _: NoSuchFunctionException => Seq(Row(s"Function: ${delegate.functionName} not found."))
        }
    }
  }
}
