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
import org.apache.spark.sql.hive.execution.{KatanaCreateHiveTableAsSelectCommand, KatanaInsertIntoHiveTable, CreateHiveTableAsSelectCommand, InsertIntoHiveTable}
import org.apache.spark.sql.hive.{KatanaContext, CatalogSchemaUtil}
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
  private val katanaSessionState: HashMap[String, SessionState] = katanaContext.katanaSessionState

  override def apply(plan: LogicalPlan): Seq[SparkPlan] = plan match {
    /**
      * 因为在Analyzer后面还有Optimizer，所以在Analyzer后面做替换会造成Optimizer错误
      *
      */

    //      在 Analyze 中已经处理过InsertInto 的Relation， 故此处不需要处理
    case _@InsertIntoHiveTable(tableMeta, partition, query, overwrite, ifPartitionNotExists, outputColumnNames) => {
      val catalog = CatalogSchemaUtil.getCatalog(hiveCatalogs, tableMeta)(sparkSession)
      val sessionState = CatalogSchemaUtil.getSessionStateByOriginTableIdentifier(katanaSessionState, tableMeta.identifier)(sparkSession)
      val katanaPlan = KatanaInsertIntoHiveTable(tableMeta, partition, query, overwrite,
        ifPartitionNotExists, outputColumnNames)(catalog, sessionState)
      DataWritingCommandExec(katanaPlan, planLater(katanaPlan.query)) :: Nil
    }

    /**
      * Create DDL
      */
    case ctas@CreateHiveTableAsSelectCommand(tableDesc, query, outputColumnNames, mode) =>
      val (catalog: SessionCatalog, originDB: String) = tableDesc.identifier.database match {
        case None => {
          val tempCatalog =
            if (katanaContext.getActiveSessionState() == null)
              sparkSession.sessionState.catalog
            else
              katanaContext.getActiveSessionState().catalog
          (tempCatalog, tempCatalog.getCurrentDatabase)
        }
        case Some(db) => CatalogSchemaUtil.getCatalogAndOriginDBName(hiveCatalogs, db)(sparkSession)
      }

      val sessionState = CatalogSchemaUtil.getSessionStateByTableIdentifier(katanaSessionState, tableDesc.identifier)(sparkSession, katanaContext)

      val originTableIdentifier = new TableIdentifier(table = tableDesc.identifier.table, database = Some(originDB))
      val originTableDesc = new CatalogTable(originTableIdentifier,
        tableDesc.tableType,
        tableDesc.storage,
        tableDesc.schema,
        tableDesc.provider,
        tableDesc.partitionColumnNames,
        tableDesc.bucketSpec,
        tableDesc.owner,
        tableDesc.createTime,
        tableDesc.lastAccessTime,
        tableDesc.createVersion,
        tableDesc.properties,
        tableDesc.stats,
        tableDesc.viewText,
        tableDesc.comment,
        tableDesc.unsupportedFeatures,
        tableDesc.tracksPartitionsInCatalog,
        tableDesc.schemaPreservesCase,
        tableDesc.ignoredProperties)
      val katanaPlan = KatanaCreateHiveTableAsSelectCommand(originTableDesc, query, outputColumnNames, mode)(catalog, sessionState)
      DataWritingCommandExec(katanaPlan, planLater(katanaPlan.query)) :: Nil


    case createTable: CreateTableCommand => ExecutedCommandExec(KatanaCreateTable(createTable, hiveCatalogs)(katanaContext)) :: Nil
    case createTableLike: CreateTableLikeCommand => ExecutedCommandExec(KatanaCreateTableLike(createTableLike, hiveCatalogs)(katanaContext)) :: Nil
    case createView: CreateViewCommand => ExecutedCommandExec(KatanaCreateView(createView, hiveCatalogs)(katanaContext)) :: Nil


    /**
      * LOAD DATA [LOCAL] INPATH
      */
    case loadData: LoadDataCommand =>
      val sessionState = CatalogSchemaUtil.getSessionStateByTableIdentifier(katanaSessionState, loadData.table)(sparkSession, katanaContext)
      ExecutedCommandExec(KatanaLoadData(loadData, hiveCatalogs)(sessionState, katanaContext)) :: Nil


    /**
      * Analyze Command
      */
    case analyzeTable: AnalyzeTableCommand =>
      val sessionState = CatalogSchemaUtil.getSessionStateByTableIdentifier(katanaSessionState, analyzeTable.tableIdent)(sparkSession, katanaContext)
      ExecutedCommandExec(KatanaAnalyzeTable(analyzeTable, hiveCatalogs)(sessionState, katanaContext)) :: Nil
    case analyzePartition: AnalyzePartitionCommand =>
      val sessionState = CatalogSchemaUtil.getSessionStateByTableIdentifier(katanaSessionState, analyzePartition.tableIdent)(sparkSession, katanaContext)
      ExecutedCommandExec(KatanaAnalyzePartition(analyzePartition, hiveCatalogs)(sessionState, katanaContext)) :: Nil
    case analyzeColumn: AnalyzeColumnCommand =>
      val sessionState = CatalogSchemaUtil.getSessionStateByTableIdentifier(katanaSessionState, analyzeColumn.tableIdent)(sparkSession, katanaContext)
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
      ExecutedCommandExec(KatanaAlterDatabaseProperties(alterDatabaseProperties, hiveCatalogs)) :: Nil

    //TABLE
    case alterTableRename: AlterTableRenameCommand =>
      ExecutedCommandExec(KatanaAlterTableRename(alterTableRename, hiveCatalogs)(katanaContext)) :: Nil
    case alterTableSerDeProperties: AlterTableSerDePropertiesCommand =>
      ExecutedCommandExec(KatanaAlterTableSerDeProperties(alterTableSerDeProperties, hiveCatalogs)(katanaContext)) :: Nil
    case alterTableSetLocation: AlterTableSetLocationCommand =>
      val sessionState = CatalogSchemaUtil.getSessionStateByTableIdentifier(katanaSessionState, alterTableSetLocation.tableName)(sparkSession, katanaContext)
      ExecutedCommandExec(KatanaAlterTableSetLocation(alterTableSetLocation, hiveCatalogs)(sessionState, katanaContext)) :: Nil
    case alterTableSetProperties: AlterTableSetPropertiesCommand =>
      ExecutedCommandExec(KatanaAlterTableSetProperties(alterTableSetProperties, hiveCatalogs)(katanaContext)) :: Nil
    case alterTableUnsetProperties: AlterTableUnsetPropertiesCommand =>
      ExecutedCommandExec(KatanaAlterTableUnsetProperties(alterTableUnsetProperties, hiveCatalogs)(katanaContext)) :: Nil

    //      PARTITION
    case alterTableAddPartition: AlterTableAddPartitionCommand =>
      val sessionState = CatalogSchemaUtil.getSessionStateByTableIdentifier(katanaSessionState, alterTableAddPartition.tableName)(sparkSession, katanaContext)
      ExecutedCommandExec(KatanaAlterTableAddPartition(alterTableAddPartition, hiveCatalogs)(sessionState, katanaContext)) :: Nil
    case alterTableDropPartition: AlterTableDropPartitionCommand =>
      val sessionState = CatalogSchemaUtil.getSessionStateByTableIdentifier(katanaSessionState, alterTableDropPartition.tableName)(sparkSession, katanaContext)
      ExecutedCommandExec(KatanaAlterTableDropPartition(alterTableDropPartition, hiveCatalogs)(sessionState, katanaContext)) :: Nil
    case alterTableRecoverPartitions: AlterTableRecoverPartitionsCommand =>
      val sessionState = CatalogSchemaUtil.getSessionStateByTableIdentifier(katanaSessionState, alterTableRecoverPartitions.tableName)(sparkSession, katanaContext)
      ExecutedCommandExec(KatanaAlterTableRecoverPartitions(alterTableRecoverPartitions, hiveCatalogs)(sessionState, katanaContext)) :: Nil
    case alterTableRenamePartition: AlterTableRenamePartitionCommand =>
      ExecutedCommandExec(KatanaAlterTableRenamePartition(alterTableRenamePartition, hiveCatalogs)(katanaContext)) :: Nil

    //      COLUMNS
    case alterTableAddColumns: AlterTableAddColumnsCommand =>
      val sessionState = CatalogSchemaUtil.getSessionStateByTableIdentifier(katanaSessionState, alterTableAddColumns.table)(sparkSession, katanaContext)
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
