package org.apache.spark.sql.hive.strategy

import org.apache.spark.sql.catalyst.TableIdentifier
import org.apache.spark.sql.catalyst.catalog.{CatalogTable, SessionCatalog}
import org.apache.spark.sql.catalyst.plans.logical.LogicalPlan
import org.apache.spark.sql.execution.SparkPlan
import org.apache.spark.sql.execution.command._
import org.apache.spark.sql.hive.execution.command.alter.columns.{KatanaAlterTableAddColumns, KatanaAlterTableChangeColumn}
import org.apache.spark.sql.hive.execution.command.alter.database.KatanaAlterDatabaseProperties
import org.apache.spark.sql.hive.execution.command.alter.partitions.{KatanaAlterTableAddPartition, KatanaAlterTableDropPartition, KatanaAlterTableRecoverPartitions, KatanaAlterTableRenamePartition}
import org.apache.spark.sql.hive.execution.command.alter.table._
import org.apache.spark.sql.hive.execution.command.alter.view.KatanaAlterViewAs
import org.apache.spark.sql.hive.execution.command.analyze.{KatanaAnalyzeColumn, KatanaAnalyzePartition, KatanaAnalyzeTable}
import org.apache.spark.sql.hive.execution.command.cache.{KatanaCacheTable, KatanaClearCache, KatanaUnCacheTable}
import org.apache.spark.sql.hive.execution.command.create.{KatanaCreateTable, KatanaCreateTableLike, KatanaCreateView}
import org.apache.spark.sql.hive.execution.command.load.KatanaLoadData
import org.apache.spark.sql.hive.execution.command.truncate.KatanaTruncateTable
import org.apache.spark.sql.hive.execution._
import org.apache.spark.sql.hive.{CatalogSchemaUtil, KatanaContext}
import org.apache.spark.sql.internal.SessionState
import org.apache.spark.sql.{SparkSession, Strategy}

import scala.collection.mutable.HashMap

/**
 * @author angers.zhu@gmail.com
 * @date 2019/5/29 9:32
 */
case class KatanaBasicOperators(getOrCreateKatanaContext: SparkSession => KatanaContext)
                               (sparkSession: SparkSession) extends Strategy {

  private val katanaContext: KatanaContext = getOrCreateKatanaContext(sparkSession)
  private val hiveCatalogs: HashMap[String, SessionCatalog] = katanaContext.hiveCatalogs
  private val katanaSessionStates: HashMap[String, SessionState] = katanaContext.katanaSessionState

  override def apply(plan: LogicalPlan): Seq[SparkPlan] = plan match {
    /**
     * 因为在Analyzer后面还有Optimizer，所以在Analyzer后面做替换会造成Optimizer错误
     *
     */

    //      在 Analyze 中已经处理过InsertInto 的Relation， 故此处不需要处理
    case InsertIntoHiveTable(tableMeta, partition, query, overwrite, ifPartitionNotExists, outputColumnNames) =>
      val catalog = CatalogSchemaUtil.getCatalog(tableMeta.identifier.catalog, hiveCatalogs, sparkSession, katanaContext)
      val sessionState = CatalogSchemaUtil.getSessionState(tableMeta.identifier.catalog, katanaSessionStates, sparkSession, katanaContext)
      val catalogName = CatalogSchemaUtil.getCatalogName(catalog, hiveCatalogs)
      val katanaPlan = KatanaInsertIntoHiveTable(tableMeta, partition, query, overwrite,
        ifPartitionNotExists, outputColumnNames)(catalog, catalogName, sessionState)
      DataWritingCommandExec(katanaPlan, planLater(katanaPlan.query)) :: Nil

    case InsertIntoHiveDirCommand(isLocal, storage, query, overwrite, outputColumnNames) =>
      val katanaPlan = KatanaInsertIntoDir(isLocal, storage, query, overwrite, outputColumnNames)
      DataWritingCommandExec(katanaPlan, planLater(katanaPlan.query)) :: Nil

    /**
     * Create DDL
     */
    case CreateHiveTableAsSelectCommand(tableDesc, query, outputColumnNames, mode) =>
      val catalog =
        CatalogSchemaUtil.getCatalog(
          tableDesc.identifier.catalog,
          hiveCatalogs,
          sparkSession,
          katanaContext)

      val sessionState =
        CatalogSchemaUtil.getSessionState(
          tableDesc.identifier.catalog,
          katanaSessionStates,
          sparkSession,
          katanaContext)

      val catalogName = CatalogSchemaUtil.getCatalogName(catalog, hiveCatalogs)
      val katanaPlan = KatanaCreateHiveTableAsSelectCommand(tableDesc, query, outputColumnNames, mode)(catalog, catalogName, sessionState)
      DataWritingCommandExec(katanaPlan, planLater(katanaPlan.query)) :: Nil

    case createTable: CreateTableCommand => ExecutedCommandExec(KatanaCreateTable(createTable, hiveCatalogs)(katanaContext)) :: Nil
    case createTableLike: CreateTableLikeCommand => ExecutedCommandExec(KatanaCreateTableLike(createTableLike, hiveCatalogs)(katanaContext)) :: Nil
    case createView: CreateViewCommand => ExecutedCommandExec(KatanaCreateView(createView, hiveCatalogs)(katanaContext)) :: Nil


    /**
     * LOAD DATA [LOCAL] INPATH
     */
    case loadData: LoadDataCommand =>
      val sessionState = CatalogSchemaUtil.getSessionState(loadData.table.catalog, katanaSessionStates, sparkSession, katanaContext)
      ExecutedCommandExec(KatanaLoadData(loadData, hiveCatalogs)(sessionState, katanaContext)) :: Nil


    /**
     * Analyze Command
     */
    case analyzeTable: AnalyzeTableCommand =>
      val sessionState = CatalogSchemaUtil.getSessionState(analyzeTable.tableIdent.catalog, katanaSessionStates, sparkSession, katanaContext)
      ExecutedCommandExec(KatanaAnalyzeTable(analyzeTable, hiveCatalogs)(sessionState, katanaContext)) :: Nil
    case analyzePartition: AnalyzePartitionCommand =>
      val sessionState = CatalogSchemaUtil.getSessionState(analyzePartition.tableIdent.catalog, katanaSessionStates, sparkSession, katanaContext)
      ExecutedCommandExec(KatanaAnalyzePartition(analyzePartition, hiveCatalogs)(sessionState, katanaContext)) :: Nil
    case analyzeColumn: AnalyzeColumnCommand =>
      val sessionState = CatalogSchemaUtil.getSessionState(analyzeColumn.tableIdent.catalog, katanaSessionStates, sparkSession, katanaContext)
      ExecutedCommandExec(KatanaAnalyzeColumn(analyzeColumn, hiveCatalogs)(sessionState, katanaContext)) :: Nil


    /**
     * Support for TRUNCATE command
     */
    case truncateTable: TruncateTableCommand =>
      ExecutedCommandExec(KatanaTruncateTable(truncateTable, hiveCatalogs)(katanaContext)) :: Nil


    /**
     * Support for ALTER command
     */
    //    DB
    case alterDatabaseProperties: AlterDatabasePropertiesCommand =>
      ExecutedCommandExec(KatanaAlterDatabaseProperties(alterDatabaseProperties, hiveCatalogs)(katanaContext)) :: Nil

    //TABLE
    case alterTableRename: AlterTableRenameCommand =>
      ExecutedCommandExec(KatanaAlterTableRename(alterTableRename, hiveCatalogs)(katanaContext)) :: Nil
    case alterTableSerDeProperties: AlterTableSerDePropertiesCommand =>
      ExecutedCommandExec(KatanaAlterTableSerDeProperties(alterTableSerDeProperties, hiveCatalogs)(katanaContext)) :: Nil
    case alterTableSetLocation: AlterTableSetLocationCommand =>
      val sessionState = CatalogSchemaUtil.getSessionState(alterTableSetLocation.tableName.catalog, katanaSessionStates, sparkSession, katanaContext)
      ExecutedCommandExec(KatanaAlterTableSetLocation(alterTableSetLocation, hiveCatalogs)(sessionState, katanaContext)) :: Nil
    case alterTableSetProperties: AlterTableSetPropertiesCommand =>
      ExecutedCommandExec(KatanaAlterTableSetProperties(alterTableSetProperties, hiveCatalogs)(katanaContext)) :: Nil
    case alterTableUnsetProperties: AlterTableUnsetPropertiesCommand =>
      ExecutedCommandExec(KatanaAlterTableUnsetProperties(alterTableUnsetProperties, hiveCatalogs)(katanaContext)) :: Nil

    //      PARTITION
    case alterTableAddPartition: AlterTableAddPartitionCommand =>
      val sessionState = CatalogSchemaUtil.getSessionState(alterTableAddPartition.tableName.catalog, katanaSessionStates, sparkSession, katanaContext)
      ExecutedCommandExec(KatanaAlterTableAddPartition(alterTableAddPartition, hiveCatalogs)(sessionState, katanaContext)) :: Nil
    case alterTableDropPartition: AlterTableDropPartitionCommand =>
      val sessionState = CatalogSchemaUtil.getSessionState(alterTableDropPartition.tableName.catalog, katanaSessionStates, sparkSession, katanaContext)
      ExecutedCommandExec(KatanaAlterTableDropPartition(alterTableDropPartition, hiveCatalogs)(sessionState, katanaContext)) :: Nil
    case alterTableRecoverPartitions: AlterTableRecoverPartitionsCommand =>
      val sessionState = CatalogSchemaUtil.getSessionState(alterTableRecoverPartitions.tableName.catalog, katanaSessionStates, sparkSession, katanaContext)
      ExecutedCommandExec(KatanaAlterTableRecoverPartitions(alterTableRecoverPartitions, hiveCatalogs)(sessionState, katanaContext)) :: Nil
    case alterTableRenamePartition: AlterTableRenamePartitionCommand =>
      ExecutedCommandExec(KatanaAlterTableRenamePartition(alterTableRenamePartition, hiveCatalogs)(katanaContext)) :: Nil

    //      COLUMNS
    case alterTableAddColumns: AlterTableAddColumnsCommand =>
      val sessionState = CatalogSchemaUtil.getSessionState(alterTableAddColumns.table.catalog, katanaSessionStates, sparkSession, katanaContext)
      ExecutedCommandExec(KatanaAlterTableAddColumns(alterTableAddColumns, hiveCatalogs)(sessionState, katanaContext)) :: Nil
    case alterTableChangeColumn: AlterTableChangeColumnCommand =>
      ExecutedCommandExec(KatanaAlterTableChangeColumn(alterTableChangeColumn, hiveCatalogs)(katanaContext)) :: Nil

    //      VIEW
    case alterViewAs: AlterViewAsCommand =>
      ExecutedCommandExec(KatanaAlterViewAs(alterViewAs, hiveCatalogs)(katanaContext)) :: Nil


    /**
     * CACHE
     */
    case cacheTable: CacheTableCommand =>
      ExecutedCommandExec(KatanaCacheTable(cacheTable, hiveCatalogs)) :: Nil
    case unCacheTable: UncacheTableCommand =>
      ExecutedCommandExec(KatanaUnCacheTable(unCacheTable, hiveCatalogs)) :: Nil
    case clearCache: ClearCacheCommand =>
      ExecutedCommandExec(KatanaClearCache(clearCache, hiveCatalogs)) :: Nil


    case _ => Nil
  }
}
