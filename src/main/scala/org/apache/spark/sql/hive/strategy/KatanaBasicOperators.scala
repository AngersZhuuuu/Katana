package org.apache.spark.sql.hive.strategy

import org.apache.spark.sql.{SparkSession, Strategy}
import org.apache.spark.sql.catalyst.plans.logical.LogicalPlan
import org.apache.spark.sql.execution.SparkPlan
import org.apache.spark.sql.execution.command._
import org.apache.spark.sql.hive.{CatalogSchemaUtil, KatanaContext}
import org.apache.spark.sql.hive.execution._
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

/**
 * @author angers.zhu@gmail.com
 * @date 2019/5/29 9:32
 */
case class KatanaBasicOperators(getOrCreateKatanaContext: SparkSession => KatanaContext)
                               (sparkSession: SparkSession) extends Strategy {

  private val katanaContext: KatanaContext = getOrCreateKatanaContext(sparkSession)

  override def apply(plan: LogicalPlan): Seq[SparkPlan] = plan match {
    /**
     * 因为在Analyzer后面还有Optimizer，所以在Analyzer后面做替换会造成Optimizer错误
     *
     */

    //      在 Analyze 中已经处理过InsertInto 的Relation， 故此处不需要处理
    case InsertIntoHiveTable(tableMeta, partition, query, overwrite, ifPartitionNotExists, outputColumnNames) =>
      val session = CatalogSchemaUtil.getSession(tableMeta.identifier.catalog, sparkSession, katanaContext)
      val catalogName = CatalogSchemaUtil.getCatalogName(session.sessionState.catalog, katanaContext)
      val katanaPlan = KatanaInsertIntoHiveTable(tableMeta, partition, query, overwrite,
        ifPartitionNotExists, outputColumnNames)(session.sessionState.catalog, catalogName, session)
      DataWritingCommandExec(katanaPlan, planLater(katanaPlan.query)) :: Nil

    case InsertIntoHiveDirCommand(isLocal, storage, query, overwrite, outputColumnNames) =>
      val katanaPlan = KatanaInsertIntoDir(isLocal, storage, query, overwrite, outputColumnNames)
      DataWritingCommandExec(katanaPlan, planLater(katanaPlan.query)) :: Nil

    /**
     * Create DDL
     */
    case CreateHiveTableAsSelectCommand(tableDesc, query, outputColumnNames, mode) =>
      val session = CatalogSchemaUtil.getSession(tableDesc.identifier.catalog, sparkSession, katanaContext)
      val katanaPlan = KatanaCreateHiveTableAsSelectCommand(tableDesc, query, outputColumnNames, mode)(session, katanaContext)
      DataWritingCommandExec(katanaPlan, planLater(katanaPlan.query)) :: Nil

    case createTable: CreateTableCommand => ExecutedCommandExec(KatanaCreateTable(createTable)(katanaContext)) :: Nil
    case createTableLike: CreateTableLikeCommand => ExecutedCommandExec(KatanaCreateTableLike(createTableLike)(katanaContext)) :: Nil
    case createView: CreateViewCommand => ExecutedCommandExec(KatanaCreateView(createView)(katanaContext)) :: Nil


    /**
     * LOAD DATA [LOCAL] INPATH
     */
    case loadData: LoadDataCommand =>
      val session = CatalogSchemaUtil.getSession(loadData.table.catalog, sparkSession, katanaContext)
      ExecutedCommandExec(KatanaLoadData(loadData)(session, katanaContext)) :: Nil


    /**
     * Analyze Command
     */
    case analyzeTable: AnalyzeTableCommand =>
      val session = CatalogSchemaUtil.getSession(analyzeTable.tableIdent.catalog, sparkSession, katanaContext)
      ExecutedCommandExec(KatanaAnalyzeTable(analyzeTable)(session, katanaContext)) :: Nil
    case analyzePartition: AnalyzePartitionCommand =>
      val sessiom = CatalogSchemaUtil.getSession(analyzePartition.tableIdent.catalog, sparkSession, katanaContext)
      ExecutedCommandExec(KatanaAnalyzePartition(analyzePartition)(sessiom, katanaContext)) :: Nil
    case analyzeColumn: AnalyzeColumnCommand =>
      val session = CatalogSchemaUtil.getSession(analyzeColumn.tableIdent.catalog, sparkSession, katanaContext)
      ExecutedCommandExec(KatanaAnalyzeColumn(analyzeColumn)(session, katanaContext)) :: Nil


    /**
     * Support for TRUNCATE command
     */
    case truncateTable: TruncateTableCommand =>
      ExecutedCommandExec(KatanaTruncateTable(truncateTable)(katanaContext)) :: Nil


    /**
     * Support for ALTER command
     */
    //    DB
    case alterDatabaseProperties: AlterDatabasePropertiesCommand =>
      ExecutedCommandExec(KatanaAlterDatabaseProperties(alterDatabaseProperties)(katanaContext)) :: Nil

    //TABLE
    case alterTableRename: AlterTableRenameCommand =>
      ExecutedCommandExec(KatanaAlterTableRename(alterTableRename)(katanaContext)) :: Nil
    case alterTableSerDeProperties: AlterTableSerDePropertiesCommand =>
      ExecutedCommandExec(KatanaAlterTableSerDeProperties(alterTableSerDeProperties)(katanaContext)) :: Nil
    case alterTableSetLocation: AlterTableSetLocationCommand =>
      val session = CatalogSchemaUtil.getSession(alterTableSetLocation.tableName.catalog, sparkSession, katanaContext)
      ExecutedCommandExec(KatanaAlterTableSetLocation(alterTableSetLocation)(session, katanaContext)) :: Nil
    case alterTableSetProperties: AlterTableSetPropertiesCommand =>
      ExecutedCommandExec(KatanaAlterTableSetProperties(alterTableSetProperties)(katanaContext)) :: Nil
    case alterTableUnsetProperties: AlterTableUnsetPropertiesCommand =>
      ExecutedCommandExec(KatanaAlterTableUnsetProperties(alterTableUnsetProperties)(katanaContext)) :: Nil

    //      PARTITION
    case alterTableAddPartition: AlterTableAddPartitionCommand =>
      val session = CatalogSchemaUtil.getSession(alterTableAddPartition.tableName.catalog, sparkSession, katanaContext)
      ExecutedCommandExec(KatanaAlterTableAddPartition(alterTableAddPartition)(session, katanaContext)) :: Nil
    case alterTableDropPartition: AlterTableDropPartitionCommand =>
      val session = CatalogSchemaUtil.getSession(alterTableDropPartition.tableName.catalog, sparkSession, katanaContext)
      ExecutedCommandExec(KatanaAlterTableDropPartition(alterTableDropPartition)(session, katanaContext)) :: Nil
    case alterTableRecoverPartitions: AlterTableRecoverPartitionsCommand =>
      val session = CatalogSchemaUtil.getSession(alterTableRecoverPartitions.tableName.catalog, sparkSession, katanaContext)
      ExecutedCommandExec(KatanaAlterTableRecoverPartitions(alterTableRecoverPartitions)(session, katanaContext)) :: Nil
    case alterTableRenamePartition: AlterTableRenamePartitionCommand =>
      ExecutedCommandExec(KatanaAlterTableRenamePartition(alterTableRenamePartition)(katanaContext)) :: Nil

    //      COLUMNS
    case alterTableAddColumns: AlterTableAddColumnsCommand =>
      val session = CatalogSchemaUtil.getSession(alterTableAddColumns.table.catalog, sparkSession, katanaContext)
      ExecutedCommandExec(KatanaAlterTableAddColumns(alterTableAddColumns)(session, katanaContext)) :: Nil
    case alterTableChangeColumn: AlterTableChangeColumnCommand =>
      ExecutedCommandExec(KatanaAlterTableChangeColumn(alterTableChangeColumn)(katanaContext)) :: Nil

    //      VIEW
    case alterViewAs: AlterViewAsCommand =>
      ExecutedCommandExec(KatanaAlterViewAs(alterViewAs)(katanaContext)) :: Nil


    /**
     * CACHE
     */
    case cacheTable: CacheTableCommand =>
      ExecutedCommandExec(KatanaCacheTable(cacheTable)) :: Nil
    case unCacheTable: UncacheTableCommand =>
      ExecutedCommandExec(KatanaUnCacheTable(unCacheTable)) :: Nil
    case clearCache: ClearCacheCommand =>
      ExecutedCommandExec(KatanaClearCache(clearCache)) :: Nil


    case _ => Nil
  }
}
