package org.apache.spark.sql.hive.execution.command.load

import java.net.URI

import org.apache.hadoop.fs.{FileContext, FsConstants, Path}
import org.apache.spark.sql.{AnalysisException, Row, SparkSession}
import org.apache.spark.sql.catalyst.catalog.CatalogTableType
import org.apache.spark.sql.execution.command.{DDLUtils, LoadDataCommand, RunnableCommand}
import org.apache.spark.sql.hive.{CatalogSchemaUtil, KatanaContext}
import org.apache.spark.sql.hive.execution.command.KatanaCommandUtils

/**
  * @author angers.zhu@gmail.com
  * @date 2019/5/30 10:45
  */
case class KatanaLoadData(delegate: LoadDataCommand)
                         (@transient private val katana: KatanaContext) extends RunnableCommand {

  override def run(sparkSession: SparkSession): Seq[Row] = {
    val session = CatalogSchemaUtil.getSession(delegate.table.catalog, sparkSession, katana)
    val catalog = session.sessionState.catalog
    val targetTable = catalog.getTableMetadata(delegate.table)
    val tableIdentifierWithDB = targetTable.identifier.quotedString

    if (targetTable.tableType == CatalogTableType.VIEW) {
      throw new AnalysisException(s"Target table in LOAD DATA cannot be a view: $tableIdentifierWithDB")
    }
    if (DDLUtils.isDatasourceTable(targetTable)) {
      throw new AnalysisException(
        s"LOAD DATA is not supported for datasource tables: $tableIdentifierWithDB")
    }
    if (targetTable.partitionColumnNames.nonEmpty) {
      if (delegate.partition.isEmpty) {
        throw new AnalysisException(s"LOAD DATA target table $tableIdentifierWithDB is partitioned, " +
          s"but no partition spec is provided")
      }
      if (targetTable.partitionColumnNames.size != delegate.partition.get.size) {
        throw new AnalysisException(s"LOAD DATA target table $tableIdentifierWithDB is partitioned, " +
          s"but number of columns in provided partition spec (${delegate.partition.get.size}) " +
          s"do not match number of partitioned columns in table " +
          s"(${targetTable.partitionColumnNames.size})")
      }
      delegate.partition.get.keys.foreach { colName =>
        if (!targetTable.partitionColumnNames.contains(colName)) {
          throw new AnalysisException(s"LOAD DATA target table $tableIdentifierWithDB is partitioned, " +
            s"but the specified partition spec refers to a column that is not partitioned: " +
            s"'$colName'")
        }
      }
    } else {
      if (delegate.partition.nonEmpty) {
        throw new AnalysisException(s"LOAD DATA target table $tableIdentifierWithDB is not " +
          s"partitioned, but a partition spec was provided.")
      }
    }
    val loadPath = {
      if (delegate.isLocal) {
        val localFS = FileContext.getLocalFSFileContext()
        LoadDataCommand.makeQualified(FsConstants.LOCAL_FS_URI, localFS.getWorkingDirectory(),
          new Path(delegate.path))
      } else {
        val loadPath = new Path(delegate.path)
        // Follow Hive's behavior:
        // If no schema or authority is provided with non-local inpath,
        // we will use hadoop configuration "fs.defaultFS".
        val defaultFSConf = sparkSession.sessionState.newHadoopConf().get("fs.defaultFS")
        val defaultFS = if (defaultFSConf == null) new URI("") else new URI(defaultFSConf)
        // Follow Hive's behavior:
        // If LOCAL is not specified, and the path is relative,
        // then the path is interpreted relative to "/user/<username>"
        val uriPath = new Path(s"/user/${System.getProperty("user.name")}/")
        // makeQualified() will ignore the query parameter part while creating a path, so the
        // entire  string will be considered while making a Path instance,this is mainly done
        // by considering the wild card scenario in mind.as per old logic query param  is
        // been considered while creating URI instance and if path contains wild card char '?'
        // the remaining charecters after '?' will be removed while forming URI instance
        LoadDataCommand.makeQualified(defaultFS, uriPath, loadPath)
      }
    }
    val fs = loadPath.getFileSystem(sparkSession.sessionState.newHadoopConf())
    // This handling is because while resolving the invalid URLs starting with file:///
    // system throws IllegalArgumentException from globStatus API,so in order to handle
    // such scenarios this code is added in try catch block and after catching the
    // runtime exception a generic error will be displayed to the user.
    try {
      val fileStatus = fs.globStatus(loadPath)
      if (fileStatus == null || fileStatus.isEmpty) {
        throw new AnalysisException(s"LOAD DATA input path does not exist: ${delegate.path}")
      }
    } catch {
      case e: IllegalArgumentException =>
        log.warn(s"Exception while validating the load path ${delegate.path} ", e)
        throw new AnalysisException(s"LOAD DATA input path does not exist: ${delegate.path}")
    }
    if (delegate.partition.nonEmpty) {
      catalog.loadPartition(
        targetTable.identifier,
        loadPath.toString,
        delegate.partition.get,
        delegate.isOverwrite,
        inheritTableSpecs = true,
        isSrcLocal = delegate.isLocal)
    } else {
      catalog.loadTable(
        targetTable.identifier,
        loadPath.toString,
        delegate.isOverwrite,
        isSrcLocal = delegate.isLocal)
    }

    // Refresh the metadata cache to ensure the data visible to the users
    catalog.refreshTable(targetTable.identifier)

    KatanaCommandUtils.updateTableStats(catalog, session, sparkSession, targetTable)
    Seq.empty[Row]
  }
}
