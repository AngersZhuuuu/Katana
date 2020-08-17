package org.apache.spark.sql.hive.analyze

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.catalyst.plans.logical.LogicalPlan
import org.apache.spark.sql.catalyst.rules.Rule
import org.apache.spark.sql.execution.command._
import org.apache.spark.sql.hive.{CatalogSchemaUtil, KatanaContext}
import org.apache.spark.sql.hive.execution.command.create.{KatanaCreateDatabase, KatanaCreateFunction}
import org.apache.spark.sql.hive.execution.command.desc.{KatanaDescColumns, KatanaDescDatabase, KatanaDescFunction, KatanaDescTable}
import org.apache.spark.sql.hive.execution.command.drop.{KatanaAlterTableDropPartition, KatanaDropDatabase, KatanaDropFunction, KatanaDropTable}
import org.apache.spark.sql.hive.execution.command.show._

/**
 * @author angers.zhu@gmail.com
 * @date 2019/5/28 15:39
 */
case class KatanaHiveDDLRule(getOrCreateKatanaContext: SparkSession => KatanaContext)
                            (sparkSession: SparkSession) extends Rule[LogicalPlan] {
  private val katanaContext: KatanaContext = getOrCreateKatanaContext(sparkSession)

  override def apply(plan: LogicalPlan): LogicalPlan = plan transformUp {
    /**
     * Support for SHOW command
     */
    case showDatabase: ShowDatabasesCommand =>
      KatanaShowDatabases(showDatabase)(katanaContext)
    case setDatabase: SetDatabaseCommand =>
      val session = CatalogSchemaUtil.getSession(setDatabase.catalog, sparkSession, katanaContext)
      KatanaSetDatabase(setDatabase)(session, katanaContext)
    case showTables: ShowTablesCommand =>
      KatanaShowTables(showTables)(katanaContext)
    case showColumns: ShowColumnsCommand =>
      KatanaShowColumns(showColumns)(katanaContext)
    case showCreateTable: ShowCreateTableCommand =>
      KatanaShowCreateTable(showCreateTable)(katanaContext)
    case showPartitions: ShowPartitionsCommand =>
      KatanaShowPartitions(showPartitions)(katanaContext)
    case showTableProperties: ShowTablePropertiesCommand =>
      KatanaShowTableProperties(showTableProperties)(katanaContext)
    case showFunctions: ShowFunctionsCommand =>
      KatanaShowFunctions(showFunctions)(katanaContext)


    /**
     * Support for Create command
     */
    case createDatabase: CreateDatabaseCommand =>
      KatanaCreateDatabase(createDatabase)(katanaContext)
    case createFunction: CreateFunctionCommand =>
      KatanaCreateFunction(createFunction)(katanaContext)


    /**
     * Support for DESC command
     */
    case descDatabase: DescribeDatabaseCommand =>
      KatanaDescDatabase(descDatabase)(katanaContext)
    case descTable: DescribeTableCommand =>
      KatanaDescTable(descTable)(katanaContext)
    case descColumn: DescribeColumnCommand =>
      KatanaDescColumns(descColumn)(katanaContext)
    case descFunction: DescribeFunctionCommand =>
      KatanaDescFunction(descFunction)(katanaContext)

    /**
     * Support for DROP command
     */
    case dropDatabase: DropDatabaseCommand =>
      KatanaDropDatabase(dropDatabase)(katanaContext)
    case dropTable: DropTableCommand =>
      KatanaDropTable(dropTable)(katanaContext)
    case dropPartition: AlterTableDropPartitionCommand =>
      KatanaAlterTableDropPartition(dropPartition)(katanaContext)
    case dropFunction: DropFunctionCommand =>
      KatanaDropFunction(dropFunction)(katanaContext)
  }

}
