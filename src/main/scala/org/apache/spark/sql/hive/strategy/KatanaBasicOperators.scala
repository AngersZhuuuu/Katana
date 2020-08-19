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
case class KatanaBasicOperators(
    getOrCreateKatanaContext: SparkSession => KatanaContext)
    (sparkSession: SparkSession) extends Strategy {

  private val katanaContext: KatanaContext = getOrCreateKatanaContext(sparkSession)

  override def apply(plan: LogicalPlan): Seq[SparkPlan] = plan match {
    /**
     * 因为在Analyzer后面还有Optimizer，所以在Analyzer后面做替换会造成Optimizer错误
     *
     */

    //      在 Analyze 中已经处理过InsertInto 的Relation， 故此处不需要处理
    case InsertIntoHiveTable(tableMeta, partition, query, overwrite, ifPartitionNotExists, outputColumnNames) =>
      val katanaPlan = KatanaInsertIntoHiveTable(tableMeta, partition, query, overwrite,
        ifPartitionNotExists, outputColumnNames)(katanaContext)
      DataWritingCommandExec(katanaPlan, planLater(katanaPlan.query)) :: Nil

    case InsertIntoHiveDirCommand(isLocal, storage, query, overwrite, outputColumnNames) =>
      val katanaPlan = KatanaInsertIntoDir(isLocal, storage, query, overwrite, outputColumnNames)(katanaContext)
      DataWritingCommandExec(katanaPlan, planLater(katanaPlan.query)) :: Nil

    /**
     * Create DDL
     */
    case CreateHiveTableAsSelectCommand(tableDesc, query, outputColumnNames, mode) =>
      val katanaPlan = KatanaCreateHiveTableAsSelectCommand(tableDesc, query, outputColumnNames, mode)(katanaContext)
      DataWritingCommandExec(katanaPlan, planLater(katanaPlan.query)) :: Nil

    case createTable: CreateTableCommand =>
      ExecutedCommandExec(KatanaCreateTable(createTable)(katanaContext)) :: Nil
    case createTableLike: CreateTableLikeCommand =>
      ExecutedCommandExec(KatanaCreateTableLike(createTableLike)(katanaContext)) :: Nil
    case createView: CreateViewCommand =>
      ExecutedCommandExec(KatanaCreateView(createView)(katanaContext)) :: Nil


    /**
     * LOAD DATA [LOCAL] INPATH
     */
    case loadData: LoadDataCommand =>
      ExecutedCommandExec(KatanaLoadData(loadData)(katanaContext)) :: Nil


    /**
     * Analyze Command
     */
    case analyzeTable: AnalyzeTableCommand =>
      ExecutedCommandExec(KatanaAnalyzeTable(analyzeTable)(katanaContext)) :: Nil
    case analyzePartition: AnalyzePartitionCommand =>
      ExecutedCommandExec(KatanaAnalyzePartition(analyzePartition)(katanaContext)) :: Nil
    case analyzeColumn: AnalyzeColumnCommand =>
      ExecutedCommandExec(KatanaAnalyzeColumn(analyzeColumn)(katanaContext)) :: Nil


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
      ExecutedCommandExec(KatanaAlterTableSetLocation(alterTableSetLocation)(katanaContext)) :: Nil
    case alterTableSetProperties: AlterTableSetPropertiesCommand =>
      ExecutedCommandExec(KatanaAlterTableSetProperties(alterTableSetProperties)(katanaContext)) :: Nil
    case alterTableUnsetProperties: AlterTableUnsetPropertiesCommand =>
      ExecutedCommandExec(KatanaAlterTableUnsetProperties(alterTableUnsetProperties)(katanaContext)) :: Nil

    //      PARTITION
    case alterTableAddPartition: AlterTableAddPartitionCommand =>
      ExecutedCommandExec(KatanaAlterTableAddPartition(alterTableAddPartition)(katanaContext)) :: Nil
    case alterTableDropPartition: AlterTableDropPartitionCommand =>
      ExecutedCommandExec(KatanaAlterTableDropPartition(alterTableDropPartition)(katanaContext)) :: Nil
    case alterTableRecoverPartitions: AlterTableRecoverPartitionsCommand =>
      ExecutedCommandExec(KatanaAlterTableRecoverPartitions(alterTableRecoverPartitions)(katanaContext)) :: Nil
    case alterTableRenamePartition: AlterTableRenamePartitionCommand =>
      ExecutedCommandExec(KatanaAlterTableRenamePartition(alterTableRenamePartition)(katanaContext)) :: Nil

    //      COLUMNS
    case alterTableAddColumns: AlterTableAddColumnsCommand =>
      ExecutedCommandExec(KatanaAlterTableAddColumns(alterTableAddColumns)(katanaContext)) :: Nil
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
