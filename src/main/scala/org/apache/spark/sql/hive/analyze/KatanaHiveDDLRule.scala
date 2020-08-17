package org.apache.spark.sql.hive.analyze

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.catalyst.catalog.SessionCatalog
import org.apache.spark.sql.catalyst.plans.logical.LogicalPlan
import org.apache.spark.sql.catalyst.rules.Rule
import org.apache.spark.sql.execution.command._
import org.apache.spark.sql.hive.{KatanaContext, CatalogSchemaUtil}
import org.apache.spark.sql.hive.execution.command.create.{KatanaCreateDatabase, KatanaCreateFunction}
import org.apache.spark.sql.hive.execution.command.desc.{KatanaDescColumns, KatanaDescDatabase, KatanaDescFunction, KatanaDescTable}
import org.apache.spark.sql.hive.execution.command.drop.{KatanaAlterTableDropPartition, KatanaDropDatabase, KatanaDropFunction, KatanaDropTable}
import org.apache.spark.sql.hive.execution.command.show._
import org.apache.spark.sql.internal.SessionState

import scala.collection.mutable.HashMap

/**
 * @author angers.zhu@gmail.com
 * @date 2019/5/28 15:39
 */
case class KatanaHiveDDLRule(getOrCreateKatanaContext: SparkSession => KatanaContext)
                            (sparkSession: SparkSession) extends Rule[LogicalPlan] {
  private val katanaContext: KatanaContext = getOrCreateKatanaContext(sparkSession)
  private val hiveCatalogs: HashMap[String, SessionCatalog] = katanaContext.hiveCatalogs
  private val katanaSessionState: HashMap[String, SessionState] = katanaContext.katanaSessionState

  override def apply(plan: LogicalPlan): LogicalPlan = plan transformUp {
    /**
     * Support for SHOW command
     */
    case showDatabase: ShowDatabasesCommand =>
      KatanaShowDatabases(showDatabase, hiveCatalogs)(katanaContext)
    case setDatabase: SetDatabaseCommand =>
      val sessionState = CatalogSchemaUtil.getSessionState(setDatabase.catalog, katanaSessionState, sparkSession, katanaContext)
      KatanaSetDatabase(setDatabase)(sessionState, katanaContext)
    case showTables: ShowTablesCommand =>
      KatanaShowTables(showTables, hiveCatalogs)(katanaContext)
    case showColumns: ShowColumnsCommand =>
      KatanaShowColumns(showColumns, hiveCatalogs)(katanaContext)
    case showCreateTable: ShowCreateTableCommand =>
      KatanaShowCreateTable(showCreateTable, hiveCatalogs)(katanaContext)
    case showPartitions: ShowPartitionsCommand =>
      KatanaShowPartitions(showPartitions, hiveCatalogs)(katanaContext)
    case showTableProperties: ShowTablePropertiesCommand =>
      KatanaShowTableProperties(showTableProperties, hiveCatalogs)(katanaContext)
    case showFunctions: ShowFunctionsCommand =>
      KatanaShowFunctions(showFunctions, hiveCatalogs)(katanaContext)


    /**
     * Support for Create command
     */
    case createDatabase: CreateDatabaseCommand =>
      KatanaCreateDatabase(createDatabase, hiveCatalogs)(katanaContext)
    case createFunction: CreateFunctionCommand =>
      KatanaCreateFunction(createFunction, hiveCatalogs)(katanaContext)


    /**
     * Support for DESC command
     */
    case descDatabase: DescribeDatabaseCommand =>
      KatanaDescDatabase(descDatabase, hiveCatalogs)(katanaContext)
    case descTable: DescribeTableCommand =>
      KatanaDescTable(descTable, hiveCatalogs)(katanaContext)
    case descColumn: DescribeColumnCommand =>
      KatanaDescColumns(descColumn, hiveCatalogs)(katanaContext)
    case descFunction: DescribeFunctionCommand =>
      KatanaDescFunction(descFunction, hiveCatalogs)(katanaContext)

    /**
     * Support for DROP command
     */
    case dropDatabase: DropDatabaseCommand =>
      KatanaDropDatabase(dropDatabase, hiveCatalogs)(katanaContext)
    case dropTable: DropTableCommand =>
      KatanaDropTable(dropTable, hiveCatalogs)(katanaContext)
    case dropPartition: AlterTableDropPartitionCommand =>
      KatanaAlterTableDropPartition(dropPartition, hiveCatalogs)(katanaContext)
    case dropFunction: DropFunctionCommand =>
      KatanaDropFunction(dropFunction, hiveCatalogs)(katanaContext)
  }

}
